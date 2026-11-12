"""Compare full row results and actual worker scans, including unsafe statistics."""

import json
import os
from datetime import date, datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

import ray
from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.datasource_v2.formats.parquet.footer_file_indexer import (
    FooterFileIndexer,
)
from ray.data.context import DataContext
from ray.data.datasource.partitioning import Partitioning, PartitionStyle
from ray.data.expressions import col


@pytest.fixture
def scanned_groups(tmp_path, monkeypatch):
    log_dir = tmp_path / "scans"
    log_dir.mkdir()

    def install_observer():
        from ray.data._internal.datasource_v2.formats.parquet.parquet_file_reader import (
            ParquetFileReader,
        )

        original = ParquetFileReader._iter_fragment_tables

        def observed(self, fragment, scanner_kwargs):
            with (log_dir / f"{os.getpid()}.jsonl").open("a") as log:
                log.write(
                    json.dumps([fragment.path, [g.id for g in fragment.row_groups]])
                    + "\n"
                )
            yield from original(self, fragment, scanner_kwargs)

        ParquetFileReader._iter_fragment_tables = observed

    monkeypatch.setenv("RAY_DATA_PARQUET_FOOTER_NUM_ACTORS", "1")
    ray.init(
        num_cpus=2,
        include_dashboard=False,
        runtime_env={"worker_process_setup_hook": install_observer},
    )
    context = DataContext.get_current()
    previous, context.use_datasource_v2 = context.use_datasource_v2, True

    def recorded():
        groups = []
        for path in log_dir.glob("*.jsonl"):
            assert int(path.stem) != os.getpid()
            for line in path.read_text().splitlines():
                file, ids = json.loads(line)
                groups.extend((file, i) for i in ids)
            path.unlink()
        assert len(groups) == len(set(groups)), "Unexpected repeated scan"
        return set(groups)

    try:
        yield recorded
    finally:
        context.use_datasource_v2 = previous
        ray.shutdown()


def compare(query, expected, monkeypatch, scanned_groups):
    actual = query()
    enabled = scanned_groups()
    configure = FooterFileIndexer.with_residual_pruning
    with monkeypatch.context() as patch:
        patch.setattr(
            FooterFileIndexer,
            "with_residual_pruning",
            lambda self, *a, **kw: configure(self),
        )
        control = query()
    disabled = scanned_groups()
    assert actual == control == expected
    assert enabled <= disabled
    return len(enabled), len(disabled)


@pytest.mark.parametrize(
    "mode,predicate,groups",
    [
        ("direct", (col("p") == "a") | (col("x") >= 25), 6),
        ("map", (col("p") == "a") | (col("x") >= 25), 6),
        ("count", (col("p") == "a") | (col("x") >= 25), 6),
        ("pushed", (col("p") == "a") | (col("x") >= 25), 6),
        ("fsspec", (col("p") == "a") | (col("x") >= 25), 6),
        ("s3", (col("p") == "a") | (col("x") >= 25), 6),
        ("null", col("p").is_null() | (col("x") >= 25), 6),
        ("missing", (col("p") == "b") | (col("x") >= 25), 6),
        ("not", ~((col("p") == "a") & (col("x") < 25)), 6),
        ("in", col("p").is_in(["a"]) | col("x").is_in([5, 35]), 6),
        ("empty", (col("p") == "absent") | (col("x") < 0), 0),
        (
            "arithmetic",
            ((col("p") == "a") & (col("x") + 100 >= 125))
            | ((col("p") == "b") & (col("x") < 0)),
            4,
        ),
        ("udf", (col("p") == "a") | (col("x") >= 25), 8),
        ("path", (col("p") == "a") | col("path").is_not_null(), 8),
    ],
)
def test_rows_and_worker_groups(
    tmp_path, monkeypatch, scanned_groups, request, mode, predicate, groups
):
    root = tmp_path / "data"
    all_rows = []
    for i, p in enumerate(("a", "b")):
        folder = "p=__NULL__" if mode == "null" and i else f"p={p}"
        if mode == "missing" and i:
            folder = "unkeyed"
        path = root / folder / "data.parquet"
        path.parent.mkdir(parents=True)
        table = pa.table(
            {
                "id": range(i * 100, i * 100 + 40),
                "x": pa.array(range(40), pa.int8()),
                # Present directory values override this; absent keys must not.
                "p": ["b"] * 40,
                "blob": [None, b"\x00\xff"] * 20,
                "time": pa.array([None, 123456789] * 20, pa.timestamp("us")),
            }
        )
        if mode == "path":
            table = table.append_column("path", pa.nulls(40, pa.string()))
        pq.write_table(table, path, row_group_size=10)
        for row in table.to_pylist():
            row["p"] = None if mode == "null" and i else p
            if mode == "udf":
                row["x"] += 100
            if mode == "path":
                row["path"] = str(path)
            all_rows.append(row)

    filesystem = None
    if mode == "fsspec":
        from fsspec.implementations.local import LocalFileSystem

        filesystem = LocalFileSystem(skip_instance_cache=True)
    elif mode == "s3":
        filesystem = request.getfixturevalue("s3_fs")
        destination = request.getfixturevalue("s3_path").removeprefix("s3://")
        pafs.copy_files(str(root), destination, destination_filesystem=filesystem)
        root = destination

    # Evaluate on all input rows before any metadata pruning.
    oracle = pa.Table.from_pylist(all_rows)
    if mode in ("pushed", "fsspec", "s3"):
        oracle = oracle.filter(pc.greater_equal(oracle["x"], 5))
    expected = ArrowBlockAccessor(oracle).filter(predicate).to_pylist()
    expected.sort(key=lambda row: row["id"])

    def transform(table):
        return table.set_column(
            table.schema.get_field_index("x"), "x", pc.add(table["x"], 100)
        )

    def query():
        ds = ray.data.read_parquet(
            str(root),
            schema=table.schema,
            filesystem=filesystem,
            partitioning=Partitioning(
                PartitionStyle.HIVE,
                base_dir=str(root),
                field_names=["p"],
                filesystem=filesystem,
                null_fallback="__NULL__",
            ),
            include_paths=mode == "path",
            _block_udf=transform if mode == "udf" else None,
        )
        if mode in ("pushed", "fsspec", "s3"):
            ds = ds.filter(expr=col("x") >= 5)
        ds = ds.filter(expr=predicate)
        if mode == "map":
            ds = ds.map_batches(lambda batch: batch, batch_format="pyarrow")
        return (
            ds.count()
            if mode == "count"
            else sorted(ds.take_all(), key=lambda row: row["id"])
        )

    assert compare(
        query,
        len(expected) if mode == "count" else expected,
        monkeypatch,
        scanned_groups,
    ) == (groups, 8)


@pytest.mark.parametrize(
    "values,dtype,reader_type,predicate,group_size,ids,groups",
    [
        (
            [float("nan"), 0.0],
            pa.float64(),
            None,
            ~(col("x") == 0.0),
            2,
            [0],
            1,
        ),
        ([float("nan"), 0.0], pa.float64(), None, col("x") != 0.0, 2, [0], 1),
        ([-0.0, 0.0], pa.float64(), None, col("x").is_in([0.0]), 2, [1], 1),
        ([100, 1], pa.int8(), None, col("x") + 100 > 127, 1, [0], 1),
        ([100, 1], pa.int8(), None, col("x").power(2) > 127, 1, [0], 2),
        (
            [datetime(2020, 1, 1, 12), datetime(2020, 1, 2)],
            pa.timestamp("us"),
            pa.date32(),
            col("x") == date(2020, 1, 1),
            2,
            [0],
            1,
        ),
        (
            [None, 1.0, 2.0, 3.0],
            pa.float64(),
            None,
            col("x").is_null(),
            2,
            [0],
            1,
        ),
        ([1, 2, 100, 101], pa.int8(), pa.int64(), col("x") >= 100, 2, [2, 3], 1),
        (
            [float("nan"), 0.0, 1.0, 2.0],
            pa.float64(),
            None,
            col("x") > 0.0,
            2,
            [2, 3],
            1,
        ),
        (
            [16777216.0, 16777218.0],
            pa.float32(),
            None,
            col("x") > 16777217.0,
            1,
            [1],
            1,
        ),
    ],
    ids=[
        "nan",
        "nan_ne",
        "signed_zero",
        "int8_add",
        "int8_power",
        "timestamp_date",
        "float_null",
        "integer_widen",
        "float_gt",
        "float32_compare",
    ],
)
def test_reader_value_semantics(
    tmp_path,
    monkeypatch,
    scanned_groups,
    values,
    dtype,
    reader_type,
    predicate,
    group_size,
    ids,
    groups,
):
    root = tmp_path / "p=other"
    root.mkdir()
    reader_type = reader_type or dtype
    table = pa.table({"id": range(len(values)), "x": pa.array(values, dtype)})
    pq.write_table(table, root / "data.parquet", row_group_size=group_size)
    schema = pa.schema([("id", pa.int64()), ("x", reader_type), ("p", pa.string())])
    expected = [
        {**row, "p": "other"} for row in table.cast(schema.remove(2)).to_pylist()
    ]

    def canonical(rows):
        return sorted(
            [
                {k: v.hex() if isinstance(v, float) else v for k, v in r.items()}
                for r in rows
            ],
            key=lambda row: row["id"],
        )

    def query():
        return canonical(
            ray.data.read_parquet(str(tmp_path), schema=schema)
            .filter(expr=(col("p") == "keep") | predicate)
            .take_all()
        )

    enabled, disabled = compare(
        query, canonical([expected[i] for i in ids]), monkeypatch, scanned_groups
    )
    assert enabled == groups
    assert disabled == len(values) // group_size


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
