"""Public API tests that observe consumed row groups inside real read workers."""
import json
import math
import os
import weakref
from collections import Counter
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq
import pytest

import ray
import ray.data
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


def _footer_indexer(value):
    from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
        FooterFileIndexer,
    )

    assert isinstance(value, FooterFileIndexer)
    return value


def _install_read_audit(directory):
    # Test-only observer: preserve native arguments, returned objects and tables.
    from ray.data._internal.datasource_v2.common.file_reader import FileReader
    from ray.data._internal.datasource_v2.formats.parquet import parquet_file_reader

    tracked = {}
    native_fragments = parquet_file_reader._fragments_from_row_group_ids
    native_read = FileReader._iter_unit_tables

    def fragments(fragment, row_group_ids, **kwargs):
        ids = [int(i) for i in row_group_ids]
        result = native_fragments(fragment, row_group_ids, **kwargs)
        assert not kwargs["per_row_group_offsets"]
        for item in result:
            key = id(item)

            def discard(reference, key=key):
                if tracked.get(key, (None,))[0] is reference:
                    tracked.pop(key)

            tracked[key] = (weakref.ref(item, discard), ids)
        return result

    def emit(event, item, ids, predicate):
        label = (Path(directory) / "label").read_text()
        record = {
            "label": label,
            "event": event,
            "q": str(predicate),
            "pid": os.getpid(),
            "path": item.unit.source,
            "groups": ids,
        }
        with (Path(directory) / f"{os.getpid()}.jsonl").open("a") as handle:
            handle.write(json.dumps(record) + "\n")

    def read(self, fragment_to_read, scanner_kwargs):
        reference, ids = tracked[id(fragment_to_read)]
        item = reference()
        assert item is fragment_to_read
        emit("start", item, ids, self._predicate)
        yield from native_read(self, fragment_to_read, scanner_kwargs)
        emit("complete", item, ids, self._predicate)
        del tracked[id(item)]

    parquet_file_reader._fragments_from_row_group_ids = fragments
    FileReader._iter_unit_tables = read


def test_listing_hint_reduces_actual_worker_reads(tmp_path, monkeypatch):
    from ray.data._internal.logical.operators import ListFiles
    from ray.data._internal.logical.rules.derive_list_files_pushdown import (
        DeriveListFilesPushdown,
    )

    audit = tmp_path / "audit"
    audit.mkdir()
    audit_path = str(audit)

    def setup_worker():
        # Resolve the worker module at execution time, as the serialization
        # contract requires; do not capture the driver's patched function.
        import importlib

        module = importlib.import_module(
            "ray.data.tests.datasource_v2.test_listing_pruning"
        )
        module._install_read_audit(audit_path)

    mode = {"enabled": False}
    native_apply = DeriveListFilesPushdown.apply

    def apply(self, plan):
        result = native_apply(self, plan)
        if not mode["enabled"]:

            def clear(node):
                if isinstance(node, ListFiles):
                    return replace(
                        node,
                        file_indexer=_footer_indexer(
                            node.file_indexer
                        ).with_listing_hint(None),
                    )
                return node

            result._dag = result.dag._apply_transform(clear)
        return result

    monkeypatch.setattr(DeriveListFilesPushdown, "apply", apply)
    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "1")
    previous_context = DataContext.get_current()
    context = DataContext()
    context.use_datasource_v2 = True
    context.enable_progress_bars = False
    context.enable_operator_progress_bars = False
    context.execution_options.preserve_order = False
    context.max_errored_blocks = 0
    DataContext._set_current(context)
    try:
        ray.init(
            num_cpus=2,
            include_dashboard=False,
            runtime_env={"worker_process_setup_hook": setup_worker},
        )
        for case in ["in", "column", "float_payload", "no_statistics"]:
            root = tmp_path / case
            expected_groups = []
            for partition in [30, 40]:
                path = root / f"p={partition}" / "data.parquet"
                path.parent.mkdir(parents=True)
                table = pa.table({"x": list(range(60))})
                if case == "float_payload":
                    table = table.append_column(
                        "f",
                        pa.array(
                            [None, float("nan"), float("inf"), -0.0] * 15,
                            type=pa.float64(),
                        ),
                    )
                pq.write_table(
                    table,
                    path,
                    row_group_size=10,
                    write_statistics=case != "no_statistics",
                )
                # Independent native Arrow oracle with known fixture values.
                expr = (
                    ds.field("x") >= ds.scalar(partition)
                    if case == "column"
                    else (ds.scalar(partition) == 99) | (ds.field("x") >= 50)
                    if case == "float_payload"
                    else (ds.scalar(partition) == 99) | ds.field("x").isin([50, 51])
                )
                fragment = ds.ParquetFileFormat().make_fragment(
                    str(path), filesystem=fs.LocalFileSystem()
                )
                ids = {
                    part.row_groups[0].id
                    for part in fragment.split_by_row_group(expr, schema=table.schema)
                }
                expected_groups.extend((str(path), i) for i in ids | {0})
            schema = table.schema.append(pa.field("p", pa.int64()))
            residual = (
                col("x") >= col("p")
                if case == "column"
                else (col("p") == 99) | (col("x") >= 50)
                if case == "float_payload"
                else (col("p") == 99) | col("x").is_in([50, 51])
            )
            expected_rows = []
            for partition in [30, 40]:
                for x in (
                    range(partition, 60)
                    if case == "column"
                    else range(50, 60)
                    if case == "float_payload"
                    else [50, 51]
                ):
                    row: dict[str, int | float | None] = {"p": partition, "x": x}
                    if case == "float_payload":
                        row["f"] = [None, float("nan"), float("inf"), -0.0][x % 4]
                    expected_rows.append(row)

            def normalize(rows):
                return [
                    {
                        k: "NaN" if isinstance(v, float) and math.isnan(v) else v
                        for k, v in row.items()
                    }
                    for row in rows
                ]

            for enabled in [False, True]:
                mode["enabled"] = enabled
                label = f"{case}-{enabled}"
                (audit / "label").write_text(label)
                materialized = (
                    ray.data.read_parquet(
                        str(root),
                        schema=schema,
                        partitioning=Partitioning(
                            PartitionStyle.HIVE,
                            base_dir=str(root),
                            field_names=["p"],
                            field_types={"p": int},
                        ),
                        override_num_blocks=1,
                    )
                    .filter(expr=residual)
                    .materialize()
                )
                rows = sorted(
                    materialized.take_all(), key=lambda row: (row["p"], row["x"])
                )
                assert normalize(rows) == normalize(expected_rows)
                assert materialized.schema().base_schema.equals(
                    schema, check_metadata=True
                )
                records = [
                    json.loads(line)
                    for path in audit.glob("*.jsonl")
                    for line in path.read_text().splitlines()
                ]
                records = [r for r in records if r["label"] == label]
                starts = [r for r in records if r["event"] == "start"]
                done = [r for r in records if r["event"] == "complete"]

                def units(events):
                    return Counter((r["path"], i) for r in events for i in r["groups"])

                assert starts and len(starts) == len(done)
                assert all(r["pid"] != os.getpid() for r in done)
                assert units(starts) == units(done)
                if enabled:
                    assert units(done) == Counter(expected_groups)
                    if case in ("column", "float_payload"):
                        assert sum(units(done).values()) < 12
                    if case == "in" and pa.__version__ == "17.0.0":
                        assert sum(units(done).values()) == 12
                else:
                    assert sum(units(done).values()) == 12
                if case == "no_statistics":
                    assert sum(units(done).values()) == 12
    finally:
        ray.shutdown()
        DataContext._set_current(previous_context)


def test_single_group_empty_residual_preserves_schema(tmp_path):
    directory = tmp_path / "p=b"
    directory.mkdir()
    pq.write_table(pa.table({"x": [1, 2, 3]}), directory / "data.parquet")
    schema = pa.schema([("x", pa.int64()), ("p", pa.string())])
    ray.init(
        num_cpus=1,
        include_dashboard=False,
        object_store_memory=128 * 1024 * 1024,
        log_to_driver=False,
    )
    context = DataContext.get_current()
    old_v2 = context.use_datasource_v2
    old_order = context.execution_options.preserve_order
    try:
        context.use_datasource_v2 = True
        context.execution_options.preserve_order = False
        result = (
            ray.data.read_parquet(
                str(tmp_path),
                schema=schema,
                partitioning=Partitioning(
                    PartitionStyle.HIVE, base_dir=str(tmp_path), field_names=["p"]
                ),
                override_num_blocks=1,
            )
            .filter(expr=(col("p") == "missing") | (col("x") > 100))
            .materialize()
        )
        assert result.count() == 0
        assert result.schema().base_schema.equals(schema, check_metadata=True)
    finally:
        context.use_datasource_v2 = old_v2
        context.execution_options.preserve_order = old_order
        ray.shutdown()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
