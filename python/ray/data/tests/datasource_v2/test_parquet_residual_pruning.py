"""Public-API residual pruning checks on a single-node Ray test cluster."""

import json
import os
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
    FooterFileIndexer,
)
from ray.data._internal.datasource_v2.formats.parquet.footer_reader import (
    _validate_predicate_references,
)
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import ColumnExpr, col, lit


@pytest.fixture
def scanned_groups(tmp_path, monkeypatch):
    log_dir = str(tmp_path / "scan-log")
    os.mkdir(log_dir)

    def install_observer():
        from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
            ParquetFileReader,
        )

        original = ParquetFileReader._iter_fragment_tables

        def observed(self, fragment, scanner_kwargs):
            # Observe scan requests, not footer prefetch or filesystem ranges.
            groups = fragment.row_groups
            assert groups is not None
            with open(os.path.join(log_dir, f"{os.getpid()}.jsonl"), "a") as log:
                log.write(json.dumps([fragment.path, [g.id for g in groups]]) + "\n")
            yield from original(self, fragment, scanner_kwargs)

        ParquetFileReader._iter_fragment_tables = observed

    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "1")
    ray.init(
        num_cpus=2,
        include_dashboard=False,
        runtime_env={
            "worker_process_setup_hook": install_observer,
        },
    )
    context = DataContext.get_current()
    previous = context.use_datasource_v2
    context.use_datasource_v2 = True

    def recorded():
        events = []
        for path in (tmp_path / "scan-log").glob("*.jsonl"):
            assert int(path.stem) != os.getpid(), "Observer ran only in the driver"
            events.extend(json.loads(line) for line in path.read_text().splitlines())
            path.unlink()  # Each query must supply its own worker evidence.
        return {(path, group) for path, groups in events for group in groups}

    try:
        yield recorded
    finally:
        context.use_datasource_v2 = previous
        ray.shutdown()


@pytest.mark.parametrize(
    "case",
    [
        "mixed",
        "unreferenced_binary_timestamp",
        "fsspec_filesystem",
        "fsspec_filesystem_q",
        "s3_filesystem",
        "s3_filesystem_q",
        "q_and_r",
        "float_q",
        "float_q_partial",
        "map",
        "count",
        "not",
        "in",
        "empty",
        "hive_null_marker",
        "configured_hive_null",
        "real_null",
        "real_null_q",
        "arithmetic",
        "column_comparison",
    ],
)
def test_residual_pruning_rows_and_worker_groups(
    tmp_path, monkeypatch, scanned_groups, request, case
):
    root = tmp_path / "data"
    all_rows: list[dict[str, Any]] = []
    partitions = (
        "a",
        "__HIVE_DEFAULT_PARTITION__"
        if case in ("hive_null_marker", "configured_hive_null")
        else "b",
    )
    for index, p in enumerate(partitions):
        p = None if case in ("real_null", "real_null_q") and index == 1 else p
        folder = root / (f"p={p}" if p is not None else "unkeyed")
        folder.mkdir(parents=True)
        table = pa.table(
            {
                "id": range(index * 100, index * 100 + 40),
                "x": range(40),
            }
        )
        if case == "unreferenced_binary_timestamp":
            # These output columns must not disable pruning on p and x.
            table = table.append_column("blob", pa.array([None, b"\x00\xff"] * 20))
            table = table.append_column(
                "timestamp",
                pa.array(
                    [None, 1_735_689_600_000_123] * 20,
                    type=pa.timestamp("us", tz="UTC"),
                ),
            )
        if case in ("float_q", "float_q_partial"):
            table = table.append_column("f", pa.array([1.0] * 40))
        if case == "configured_hive_null":
            # A present NULL folder value overrides contradictory physical data.
            table = table.append_column("p", pa.array(["physical"] * 40))
        pq.write_table(table, folder / "data.parquet", row_group_size=10)
        if case == "configured_hive_null" and index == 1:
            p = None
        all_rows.extend({**row, "p": p} for row in table.to_pylist())
    residual = (col("p") == "a") | (col("x") >= 25)
    if case in ("hive_null_marker", "configured_hive_null", "real_null", "real_null_q"):
        # Ray's default Hive marker remains a string; the unkeyed folder
        # in real_null instead has a missing field filled with actual nulls.
        residual = col("p").is_null() | (col("x") >= 25)
    elif case == "arithmetic":
        residual = ((col("p") == "a") & (col("x") + 100 >= 125)) | (
            (col("p") == "b") & (col("x") < 0)
        )
    elif case == "column_comparison":
        residual = ((col("p") == "a") & (col("x") == col("id"))) | (
            (col("p") == "b") & (col("x") < 0)
        )
    elif case == "not":
        residual = ~((col("p") == "a") & (col("x") < 25))
    elif case == "in":
        residual = col("p").is_in(["a"]) | col("x").is_in([5, 35])
    elif case == "empty":
        residual = ((col("p") == "a") & (col("x") > 100)) | (
            (col("p") == "b") & (col("x") < 0)
        )

    def matches(row):
        if case in (
            "hive_null_marker",
            "configured_hive_null",
            "real_null",
            "real_null_q",
        ):
            return row["p"] is None or row["x"] >= 25
        if case == "arithmetic":
            return row["p"] == "a" and row["x"] + 100 >= 125
        if case == "column_comparison":
            return row["p"] == "a" and row["x"] == row["id"]
        if case == "not":
            return not (row["p"] == "a" and row["x"] < 25)
        if case == "in":
            return row["p"] == "a" or row["x"] in (5, 35)
        if case == "empty":
            return False
        return row["p"] == "a" or row["x"] >= 25

    filesystem = None
    if case in ("fsspec_filesystem", "fsspec_filesystem_q"):
        from fsspec.implementations.local import LocalFileSystem

        # The public reader adds PyFileSystem and Ray's retrying wrapper.
        filesystem = LocalFileSystem(skip_instance_cache=True)

    if case.startswith("s3_filesystem"):
        filesystem = request.getfixturevalue("s3_fs")
        destination = request.getfixturevalue("s3_path").removeprefix("s3://")
        pafs.copy_files(str(root), destination, destination_filesystem=filesystem)
        root = destination

    has_q = case in (
        "q_and_r",
        "real_null_q",
        "fsspec_filesystem_q",
        "s3_filesystem_q",
        "float_q_partial",
        "configured_hive_null",
    )

    expected = [row for row in all_rows if matches(row)]
    if has_q:
        expected = [row for row in expected if row["x"] >= 5]

    def query():
        ds = ray.data.read_parquet(
            str(root),
            schema=pa.schema(
                [field for field in table.schema if field.name != "p"]
            ).append(pa.field("p", pa.string())),
            filesystem=filesystem,
            partitioning=Partitioning(
                PartitionStyle.HIVE,
                base_dir=str(root),
                field_names=["p"],
                filesystem=filesystem,
                null_fallback="__HIVE_DEFAULT_PARTITION__"
                if case == "configured_hive_null"
                else None,
            ),
        )
        if case in ("float_q", "float_q_partial"):
            ds = ds.filter(expr=col("f") > 0.0)
        if has_q:
            ds = ds.filter(expr=col("x") >= 5)
        ds = ds.filter(expr=residual)
        if case == "map":
            ds = ds.map_batches(lambda batch: batch, batch_format="pyarrow")
        return (
            ds.count()
            if case == "count"
            else sorted(ds.take_all(), key=lambda row: row["id"])
        )

    actual, control, enabled, disabled = _run_with_and_without_hint(
        query, monkeypatch, scanned_groups
    )
    assert actual == control == (len(expected) if case == "count" else expected)
    expected_groups = {
        "empty": 0,
        "hive_null_marker": 4,
        "arithmetic": 4,
        "column_comparison": 4,
    }.get(case, 6)
    assert len(enabled) == expected_groups
    assert enabled < disabled
    assert len(disabled) == 8


def _run_with_and_without_hint(query, monkeypatch, scanned_groups):
    actual = query()
    enabled = scanned_groups()
    configure = FooterFileIndexer.with_residual_pruning
    with monkeypatch.context() as patch:
        patch.setattr(
            FooterFileIndexer,
            "with_residual_pruning",
            lambda self, *args, **kwargs: configure(self),
        )
        control = query()
    return actual, control, enabled, scanned_groups()


@pytest.mark.parametrize("with_float_q", [False, True])
@pytest.mark.parametrize(
    "values,predicate,selected,dtype",
    [
        ([float("nan"), 0.0], ~(col("x") == 0.0), [0], pa.float64()),
        ([-0.0, 0.0], col("x").is_in([0.0]), [1], pa.float64()),
        ([100, 1], col("x") + 100 > 127, [0], pa.int8()),
    ],
    ids=["nan", "signed_zero_in", "int8_add"],
)
def test_residual_value_semantics(
    tmp_path,
    monkeypatch,
    scanned_groups,
    values,
    predicate,
    selected,
    dtype,
    with_float_q,
):
    # Arrow 23 eager IN distinguishes signed zeros; preserve their sign below.
    residual = (col("p") == "keep") | predicate
    root = tmp_path / "unsafe"
    expected = []
    for index, partition in enumerate(("a", "b")):
        folder = root / f"p={partition}"
        folder.mkdir(parents=True)
        table = pa.table(
            {
                "id": [index * 100, index * 100 + 1],
                "x": pa.array(values, type=dtype),
            }
        )
        if with_float_q:
            table = table.append_column("f", pa.array([1.0] * 2))
        pq.write_table(table, folder / "data.parquet", row_group_size=2)
        expected.extend({**table.to_pylist()[i], "p": partition} for i in selected)

    def query():
        ds = ray.data.read_parquet(str(root))
        if with_float_q:
            ds = ds.filter(expr=col("f") > 0.0)
        return ds.filter(expr=residual).take_all()

    def canonical(rows):
        return sorted(
            [
                {k: v.hex() if isinstance(v, float) else v for k, v in row.items()}
                for row in rows
            ],
            key=lambda row: row["id"],
        )

    actual, control, enabled, disabled = _run_with_and_without_hint(
        query, monkeypatch, scanned_groups
    )
    assert canonical(actual) == canonical(control) == canonical(expected)
    # A positive exact count rejects missing logs; equality alone would not.
    assert enabled == disabled
    assert len(enabled) == 2


@pytest.mark.parametrize("layout", ["partial", "full"])
@pytest.mark.parametrize("q_kind", ["none", "p_or_x", "q_or_x", "p_and_x"])
@pytest.mark.parametrize("r_kind", ["q_or_x", "equal_columns", "arithmetic"])
def test_partial_facts_worker(
    tmp_path, monkeypatch, scanned_groups, q_kind, r_kind, layout
):
    root = tmp_path / "data"
    all_rows = []
    for index, (folder, overrides) in enumerate(
        [
            ("root", {}),
            ("p=1", {"p": 1}) if layout == "partial" else ("p=1/q=2", {"p": 1, "q": 2}),
        ]
    ):
        path = root / folder
        path.mkdir(parents=True)
        table = pa.table(
            {
                "id": range(index * 100, index * 100 + 40),
                "x": range(40),
                "p": [3] * 40,
                "q": [1] * 20 + [3] * 20,
            }
        )
        pq.write_table(table, path / "data.parquet", row_group_size=10)
        all_rows.extend({**row, **overrides} for row in table.to_pylist())
    q = {
        "none": None,
        "p_or_x": (col("p") == 1) | (col("x") >= 5),
        "q_or_x": (col("q") == 2) | (col("x") >= 5),
        "p_and_x": (col("p") == 1) & (col("x") >= 5),
    }[q_kind]
    r = {
        "q_or_x": (col("q") == 2) | (col("x") >= 25),
        "equal_columns": col("p") == col("q"),
        "arithmetic": (col("p") == 1) & (col("q") + col("x") >= 27),
    }[r_kind]
    expected = []
    for row in all_rows:
        p, qv, x = row["p"], row["q"], row["x"]
        qm = {
            "none": True,
            "p_or_x": p == 1 or x >= 5,
            "q_or_x": qv == 2 or x >= 5,
            "p_and_x": p == 1 and x >= 5,
        }[q_kind]
        rm = {
            "q_or_x": qv == 2 or x >= 25,
            "equal_columns": p == qv,
            "arithmetic": p == 1 and qv + x >= 27,
        }[r_kind]
        if qm and rm:
            expected.append(row)

    def query():
        ds = ray.data.read_parquet(
            str(root),
            schema=table.schema,
            partitioning=Partitioning(PartitionStyle.HIVE, base_dir=str(root)),
        )
        if q is not None:
            ds = ds.filter(expr=q)
        return sorted(ds.filter(expr=r).take_all(), key=lambda row: row["id"])

    actual, control, enabled, disabled = _run_with_and_without_hint(
        query, monkeypatch, scanned_groups
    )
    assert actual == control == expected
    assert not expected or (enabled and disabled)
    assert enabled <= disabled
    # Optimizer may move a wholly convertible predicate into Q; only require
    # incremental pruning for the mixed partition/physical OR that remains R.
    if (layout == "full" and r_kind == "q_or_x" and q_kind != "p_and_x") or (
        layout == "partial" and r_kind == "equal_columns"
    ):
        assert enabled < disabled
    print(
        "PARTIAL_RESULT",
        layout,
        q_kind,
        r_kind,
        "groups",
        len(enabled),
        len(disabled),
        "rows",
        len(actual),
    )


@pytest.mark.parametrize(
    "expr",
    [
        col("f") > 0.0,
        (~(col("f") > 0.0)).alias("q"),
        (col("f") + 1.0) * 2.0 > 0.0,
        col("f").is_in([0.0, 1.0]),
        lit(True),
    ],
    ids=["float", "alias", "arithmetic", "membership", "literal"],
)
def test_q_transparent_references(expr):
    _validate_predicate_references(expr)


@pytest.mark.parametrize(
    "expr",
    [
        col("x").power(2),
        col("x").power(pc.field("p")),
        col(cast(Any, 0)),
        col(cast(Any, ("p", "x"))),
        lit(pc.field("p")),
    ],
    ids=["compute", "hidden_field", "field_index", "nested_selector", "native_literal"],
)
def test_q_opaque_references_are_rejected(expr):
    with pytest.raises((ValueError, TypeError, pa.ArrowInvalid)):
        _validate_predicate_references(expr)


def test_q_unknown_node_subclass_is_rejected():
    class CustomColumn(ColumnExpr):
        def to_pyarrow(self):
            return pc.field("hidden")

    with pytest.raises(ValueError):
        _validate_predicate_references(CustomColumn("visible"))


@pytest.mark.parametrize(
    "case",
    [
        "tensor",
        "tensor_q",
        "tensor_float_q",
        "tensor_referenced",
        "custom_udf",
        "tensor_and_custom",
    ],
)
def test_tensor_decoding_residual_pruning(tmp_path, monkeypatch, scanned_groups, case):
    root = tmp_path / "tensor"
    dtype, shape = (
        (np.float64, (2, 2)) if case == "tensor_float_q" else (np.int32, (2, 3))
    )
    has_tensor = case != "custom_udf"
    has_custom = case in ("custom_udf", "tensor_and_custom")
    expected = []
    for index, partition in enumerate(("a", "b")):
        folder = root / f"p={partition}"
        folder.mkdir(parents=True)
        ids = list(range(index * 100, index * 100 + 40))
        tensors = [
            (np.arange(np.prod(shape)).reshape(shape) + i).astype(dtype) for i in ids
        ]
        table = pa.table(
            {
                "id": ids,
                "x": range(40),
                "f": [1.0] * 40,
                "tensor": [t.tobytes() for t in tensors],
            }
        )
        pq.write_table(table, folder / "data.parquet", row_group_size=10)
        for row, tensor in zip(table.to_pylist(), tensors):
            if has_tensor:
                row["tensor"] = tensor
            if has_custom:
                row["x"] += 100
            if (partition == "a" or row["x"] >= 25) and (
                case != "tensor_q" or row["x"] >= 5
            ):
                expected.append({**row, "p": partition})

    def custom_udf(table):
        # An unknown UDF makes original low-x groups necessary after reading.
        return table.set_column(
            table.schema.get_field_index("x"),
            "x",
            pc.add(table["x"], 100),
        )

    residual = (col("p") == "a") | (col("x") >= 25)
    if case == "tensor_referenced":
        residual = residual & col("tensor").is_not_null()

    def query():
        ds = ray.data.read_parquet(
            str(root),
            schema=table.schema.append(pa.field("p", pa.string())),
            partitioning=Partitioning(
                PartitionStyle.HIVE, base_dir=str(root), field_names=["p"]
            ),
            tensor_column_schema={"tensor": (np.dtype(dtype), shape)}
            if has_tensor
            else None,
            _block_udf=custom_udf if has_custom else None,
        )
        if case == "tensor_q":
            ds = ds.filter(expr=col("x") >= 5)
        elif case == "tensor_float_q":
            ds = ds.filter(expr=col("f") > 0.0)
        return ds.filter(expr=residual).take_all()

    def canonical(rows):
        return sorted(
            [
                {
                    **row,
                    "tensor": (value.dtype.str, value.shape, value.tolist())
                    if isinstance(value := row["tensor"], np.ndarray)
                    else value,
                }
                for row in rows
            ],
            key=lambda row: row["id"],
        )

    actual, control, enabled, disabled = _run_with_and_without_hint(
        query, monkeypatch, scanned_groups
    )
    assert canonical(actual) == canonical(control) == canonical(expected)
    assert len(disabled) == 8
    if has_custom or case == "tensor_referenced":
        assert enabled == disabled
    else:
        assert len(enabled) == 6 and enabled < disabled
    # Pure decoding preserves valid values on other columns. Pruned groups
    # can still skip malformed-tensor decode errors; this is not error equivalence.


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
