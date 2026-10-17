#!/usr/bin/env python3
"""Reproducible TPC-H Q3 ELT, Iceberg exercises and format/codec benchmark."""
from __future__ import annotations

import argparse
import csv
from decimal import Decimal
import json
import os
from pathlib import Path
import random
import re
import shutil
import statistics
import traceback

from client import Trino, http_json, now, split_sql, wait_http, write_json

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "results"
TABLES = ("customer", "orders", "lineitem")
COLUMNS = {
    "customer": "custkey, name, mktsegment",
    "orders": "orderkey, custkey, orderdate, shippriority",
    "lineitem": "orderkey, linenumber, quantity, extendedprice, discount, shipdate",
}
MATRIX = [(fmt, codec) for fmt in ("PARQUET", "ORC")
          for codec in ("NONE", "SNAPPY", "GZIP", "ZSTD")]
SEED = 20261017
TOLERANCE = Decimal("0.000001")


def say(message: str) -> None:
    print(f"[{now()[11:19]} UTC] {message}", flush=True)


def sql_file(name: str, **values) -> str:
    text = (ROOT / "sql" / name).read_text(encoding="utf-8")
    for key, value in values.items():
        text = text.replace("{{" + key + "}}", str(value))
    if "{{" in text:
        raise ValueError(f"Незаполненный параметр в {name}")
    return text.strip().rstrip(";")


def schema_name(fmt: str, codec: str) -> str:
    return f"bench_{fmt.lower()}_{codec.lower()}"


def assert_q3(actual: list[dict], expected: list[dict]) -> None:
    """Compare by business key, with a stated tolerance for DOUBLE vs DECIMAL."""
    def keyed(rows):
        result = {r["orderkey"]: r for r in rows}
        if len(result) != len(rows):
            raise AssertionError("Повторяющийся orderkey в Q3")
        return result
    left, right = keyed(actual), keyed(expected)
    if left.keys() != right.keys():
        raise AssertionError(f"Не совпали ключи Q3: {left.keys() ^ right.keys()}")
    for key, row in left.items():
        reference = right[key]
        if (row["orderdate"], row["shippriority"]) != (reference["orderdate"], reference["shippriority"]):
            raise AssertionError(f"Не совпали реквизиты заказа {key}")
        if abs(Decimal(str(row["revenue"])) - Decimal(str(reference["revenue"]))) > TOLERANCE:
            raise AssertionError(f"Не совпала выручка заказа {key}")


def export_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


class Pipeline:
    def __init__(self, directory: Path, args):
        self.directory, self.args = directory, args
        self.db = Trino(directory / "queries.jsonl")

    def save(self, name: str, value) -> None:
        write_json(self.directory / name, value)

    def load(self, name: str):
        return json.loads((self.directory / name).read_text(encoding="utf-8"))

    def create(self, table: str, select: str, fmt="PARQUET", codec="ZSTD"):
        self.db.execute(f"DROP TABLE IF EXISTS {table}", "drop-owned-table")
        return self.db.execute(
            f"CREATE TABLE {table} WITH (format='{fmt}', compression_codec='{codec}') AS {select}",
            "write:" + table,
        )

    def table_info(self, table: str) -> dict:
        schema, name = table.split(".")[-2:]
        files = self.db.execute(
            f'SELECT count(*) AS data_files, coalesce(sum(record_count), 0) AS records_in_files, '
            f'coalesce(sum(file_size_in_bytes), 0) AS data_bytes '
            f'FROM lakehouse.{schema}."{name}$files" WHERE content=0', "active-file-sizes").records()[0]
        files["table"] = table
        files["rows"] = self.db.execute(f"SELECT count(*) FROM {table}", "table-row-count").scalar()
        if files["rows"] != files["records_in_files"]:
            raise AssertionError(f"Число строк в файлах не совпало: {table}")
        files["ddl"] = self.db.execute(f"SHOW CREATE TABLE {table}", "table-ddl").scalar()
        return files

    def fingerprint(self, schema: str, table: str) -> dict:
        return self.db.execute(
            f"SELECT count(*) AS rows, to_hex(checksum(ROW({COLUMNS[table]}))) AS checksum "
            f"FROM lakehouse.{schema}.{table}", "source-fingerprint").records()[0]

    def init(self):
        say("Ожидание Trino и Lakekeeper")
        trino = wait_http("http://localhost:8080/v1/info")
        base = "http://localhost:8181/management/v1"
        info = wait_http(base + "/info")
        if not info["bootstrapped"]:
            http_json(base + "/bootstrap", "POST", {"accept-terms-of-use": True})
            info = http_json(base + "/info")
        project = info.get("default-project-id") or "00000000-0000-0000-0000-000000000000"
        headers = {"x-project-id": project}
        warehouses = http_json(base + "/warehouse", headers=headers).get("warehouses", [])
        warehouse = next((w for w in warehouses if w["name"] == "practice1"), None)
        if warehouse is None:
            warehouse = http_json(base + "/warehouse", "POST", {
                "warehouse-name": "practice1", "project-id": project,
                "delete-profile": {"type": "hard"},
                "storage-profile": {
                    "type": "s3", "bucket": "lakehouse", "key-prefix": "practice1",
                    "endpoint": "http://minio:9000", "region": "us-east-1",
                    "path-style-access": True, "flavor": "minio", "sts-enabled": False,
                },
                "storage-credential": {"type": "s3", "credential-type": "access-key",
                                       "aws-access-key-id": "minioadmin",
                                       "aws-secret-access-key": "minioadmin"},
            }, headers)
        self.save("services.json", {"trino": trino, "lakekeeper": info,
                                    "warehouse_id": warehouse.get("id") or warehouse["warehouse-id"], "at": now()})
        for schema in ("bronze", "silver", "gold", "sandbox"):
            self.db.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{schema}")
        self.db.execute("SELECT 1 FROM tpch." + self.args.scale + ".customer LIMIT 1")
        say("Каталог practice1 готов")

    def elt(self):
        say("Bronze: загрузка проекций customer, orders, lineitem из " + self.args.scale)
        for table in ("shipping_priority",):
            self.db.execute(f"DROP TABLE IF EXISTS lakehouse.gold.{table}")
        for table in ("shipping_lines", "lineitem", "orders", "customer", "rejected_records"):
            self.db.execute(f"DROP TABLE IF EXISTS lakehouse.silver.{table}")
        bronze = []
        for table in TABLES:
            query = self.create("lakehouse.bronze." + table,
                                sql_file(f"bronze/{table}.sql", scale=self.args.scale))
            details = self.table_info("lakehouse.bronze." + table)
            details["initial_write_ms"] = query.elapsed_ms
            bronze.append(details)
            say(f"Bronze {table}: {details['rows']:,} строк, {details['data_bytes'] / 2**20:.2f} MiB")
        self.save("bronze.json", bronze)

        checks = {}
        for table, key, valid_key in (("customer", "custkey", "custkey IS NOT NULL"),
                                      ("orders", "orderkey", "orderkey IS NOT NULL"),
                                      ("lineitem", "ROW(orderkey, linenumber)",
                                       "orderkey IS NOT NULL AND linenumber IS NOT NULL")):
            duplicates = self.db.execute(
                f"SELECT count(*) - count(DISTINCT {key}) FROM lakehouse.bronze.{table} WHERE {valid_key}",
                "unique-business-keys").scalar()
            checks[table + "_duplicate_keys"] = duplicates
            if duplicates:
                raise AssertionError(f"Конфликтующие ключи в {table}: {duplicates}")

        say("Silver: контроль качества, нормализация типов, соединение и обогащение")
        for statement in split_sql(sql_file("silver.sql")):
            self.db.execute(statement, "silver-transform")
        checks["rejections"] = self.db.execute(
            "SELECT source_table, rejection_reason, count(*) AS rows "
            "FROM lakehouse.silver.rejected_records GROUP BY 1, 2 ORDER BY 1, 2").records()
        checks["normalized_segments"] = self.db.execute(
            "SELECT count(*) FROM lakehouse.bronze.customer "
            "WHERE mktsegment <> upper(trim(mktsegment))").scalar()
        for table in TABLES:
            rejected = sum(r["rows"] for r in checks["rejections"] if r["source_table"] == table)
            raw = next(r["rows"] for r in bronze if r["table"].endswith("." + table))
            accepted = self.db.execute(f"SELECT count(*) FROM lakehouse.silver.{table}").scalar()
            if raw != accepted + rejected:
                raise AssertionError(f"Нарушен баланс строк: {table}")
            checks[table + "_row_balance"] = {"bronze": raw, "accepted": accepted, "rejected": rejected}
        self.save("quality.json", checks)
        self.save("silver.json", [self.table_info("lakehouse.silver." + table) for table in
                                 (*TABLES, "shipping_lines", "rejected_records")])
        self.db.execute(sql_file("gold.sql"), "gold-aggregation")
        self.save("gold.json", [self.table_info("lakehouse.gold.shipping_priority")])
        self.verify()

    def verify(self):
        say("Проверка Gold по исходному Q3 на tpch")
        reference = self.db.execute(sql_file("q3.sql", source="tpch." + self.args.scale), "q3-reference")
        actual = self.db.execute(
            "SELECT orderkey, revenue, orderdate, shippriority FROM lakehouse.gold.shipping_priority "
            "ORDER BY revenue DESC, orderkey", "q3-gold")
        assert_q3(actual.records(), reference.records())
        # An independent exact-decimal reference also checks the arithmetic, in both directions.
        exact = sql_file("q3.sql", source="tpch." + self.args.scale).replace(
            "sum(l.extendedprice * (1 - l.discount))",
            "sum(cast(l.extendedprice AS DECIMAL(15,2)) * "
            "(DECIMAL '1.00' - cast(l.discount AS DECIMAL(4,2))))")
        # Trino inlines WITH relations. Materialize the ten exact rows once so the
        # two EXCEPT directions do not concurrently regenerate and join sf1 twice.
        exact_query = self.create("lakehouse.sandbox.q3_exact_reference", exact)
        differences = self.db.execute(
            "WITH reference AS (SELECT orderkey, revenue, orderdate, shippriority "
            "FROM lakehouse.sandbox.q3_exact_reference), actual AS (SELECT orderkey, revenue, orderdate, "
            "shippriority FROM lakehouse.gold.shipping_priority) "
            "SELECT 'reference_minus_gold' AS direction, count(*) AS differences "
            "FROM (SELECT * FROM reference EXCEPT ALL SELECT * FROM actual) "
            "UNION ALL SELECT 'gold_minus_reference', count(*) "
            "FROM (SELECT * FROM actual EXCEPT ALL SELECT * FROM reference)", "exact-gold-equality").records()
        if any(r["differences"] for r in differences) or len(actual.rows) != 10:
            raise AssertionError("Gold не совпал с эталоном")
        self.save("reference.json", reference.records())
        self.save("gold-result.json", actual.records())
        export_csv(self.directory / "gold-top10.csv", actual.records())
        self.save("validation.json", {"passed": True, "reference_query_id": reference.query_id,
                                     "exact_reference_query_id": exact_query.query_id,
                                     "double_absolute_tolerance": str(TOLERANCE),
                                     "exact_decimal_comparison": differences, "gold_rows": len(actual.rows)})
        say("Gold: 10 строк, двусторонний EXCEPT ALL = 0, исходный Q3 совпал")

    def benchmark(self):
        say("Бенчмарк: 8 комбинаций формата/кодека")
        reference = self.load("reference.json")
        fingerprints = {t: self.fingerprint("bronze", t) for t in TABLES}
        self.save("source-fingerprints.json", fingerprints)
        writes, queries = [], []
        rng = random.Random(SEED)
        for fmt, codec in MATRIX:
            self.db.execute(f"CREATE SCHEMA IF NOT EXISTS lakehouse.{schema_name(fmt, codec)}")
        # Round 0 warms writer classes and source pages. Every variant gets the same treatment.
        for repeat in range(self.args.write_repeats + 1):
            order = MATRIX.copy()
            rng.shuffle(order)
            for fmt, codec in order:
                schema = schema_name(fmt, codec)
                elapsed, wall, ids = 0, 0.0, []
                for table in TABLES:
                    query = self.create(f"lakehouse.{schema}.{table}",
                                        f"SELECT {COLUMNS[table]} FROM lakehouse.bronze.{table}", fmt, codec)
                    elapsed += query.elapsed_ms
                    wall += query.client_wall_ms
                    ids.append(query.query_id)
                sizes = self.db.execute(
                    " UNION ALL ".join(
                        f"SELECT '{t}' AS table_name, count(*) AS data_files, "
                        f"coalesce(sum(file_size_in_bytes),0) AS data_bytes "
                        f'FROM lakehouse.{schema}."{t}$files" WHERE content=0' for t in TABLES),
                    "benchmark-file-sizes").records()
                writes.append({"format": fmt, "codec": codec, "repeat": repeat,
                               "warmup": repeat == 0, "server_elapsed_ms": elapsed,
                               "client_wall_ms": round(wall, 3),
                               "data_bytes": sum(s["data_bytes"] for s in sizes),
                               "data_files": sum(s["data_files"] for s in sizes),
                               "tables": sizes, "query_ids": ids})
                self.save("benchmark-writes.json", writes)
                say(f"Запись {fmt}/{codec}, прогон {repeat}: {elapsed} мс, "
                    f"{writes[-1]['data_bytes'] / 2**20:.2f} MiB" + (" (прогрев)" if repeat == 0 else ""))

        copies = []
        for fmt, codec in MATRIX:
            schema = schema_name(fmt, codec)
            for table in TABLES:
                fingerprint = self.fingerprint(schema, table)
                if fingerprint != fingerprints[table]:
                    raise AssertionError(f"Содержимое отличается: {schema}.{table}")
                copies.append({"schema": schema, "table": table, **fingerprint})
        self.save("benchmark-copy-validation.json", copies)

        # Warm each variant once, then interleave measured rounds in deterministic random order.
        for repeat in range(self.args.query_repeats + 1):
            order = MATRIX.copy()
            rng.shuffle(order)
            for fmt, codec in order:
                query = self.db.execute(sql_file("q3.sql", source="lakehouse." + schema_name(fmt, codec)),
                                        "benchmark-q3-warmup" if repeat == 0 else "benchmark-q3")
                assert_q3(query.records(), reference)
                queries.append({"format": fmt, "codec": codec, "repeat": repeat,
                                "warmup": repeat == 0, "query_id": query.query_id,
                                "server_elapsed_ms": query.elapsed_ms,
                                "client_wall_ms": round(query.client_wall_ms, 3), "stats": query.stats})
                self.save("benchmark-queries.json", queries)
                say(f"Q3 {fmt}/{codec}, прогон {repeat}: {query.elapsed_ms} мс" +
                    (" (прогрев)" if repeat == 0 else ""))
        summary = []
        for fmt, codec in MATRIX:
            w = [r for r in writes if r["format"] == fmt and r["codec"] == codec and not r["warmup"]]
            q = [r for r in queries if r["format"] == fmt and r["codec"] == codec and not r["warmup"]]
            times = [r["server_elapsed_ms"] for r in q]
            summary.append({"format": fmt, "codec": codec,
                            "size_bytes_median": statistics.median(r["data_bytes"] for r in w),
                            "write_ms_median": statistics.median(r["server_elapsed_ms"] for r in w),
                            "query_ms_median": statistics.median(times),
                            "query_ms_min": min(times), "query_ms_max": max(times),
                            "query_ms_stdev": statistics.stdev(times) if len(times) > 1 else 0,
                            "write_repeats": len(w), "query_repeats": len(q)})
        self.save("benchmark-summary.json", summary)
        export_csv(self.directory / "benchmark-summary.csv", summary)
        self.save("benchmark-ddl.json", {schema_name(f, c): self.db.execute(
            f"SHOW CREATE TABLE lakehouse.{schema_name(f,c)}.lineitem").scalar() for f, c in MATRIX})

    def iceberg(self):
        say("Iceberg: изменение схемы и восстановление из снапшота в отдельной копии Silver")
        table = "lakehouse.sandbox.shipping_lines_demo"
        self.create(table, "SELECT * FROM lakehouse.silver.shipping_lines")
        before = self.table_info(table)
        files_sql = 'SELECT file_path, file_size_in_bytes FROM lakehouse.sandbox."shipping_lines_demo$files" ORDER BY file_path'
        files_before = self.db.execute(files_sql).records()
        self.db.execute(f"ALTER TABLE {table} ADD COLUMN quality_note varchar", "schema-add-column")
        self.db.execute(f"ALTER TABLE {table} RENAME COLUMN net_revenue TO discounted_revenue", "schema-rename-column")
        after = self.table_info(table)
        files_after = self.db.execute(files_sql).records()
        if files_before != files_after:
            raise AssertionError("ALTER TABLE неожиданно переписал data files")
        nulls = self.db.execute(f"SELECT count(*) FROM {table} WHERE quality_note IS NULL").scalar()
        if nulls != before["rows"]:
            raise AssertionError("Новая колонка не заполнилась NULL")
        sample = self.db.execute(f"SELECT orderkey, discounted_revenue, quality_note FROM {table} "
                                 "ORDER BY orderkey, linenumber LIMIT 5").records()
        snapshot = self.db.execute(
            'SELECT snapshot_id FROM lakehouse.sandbox."shipping_lines_demo$snapshots" '
            'ORDER BY committed_at DESC LIMIT 1').scalar()
        self.db.execute(f"DELETE FROM {table} WHERE orderkey % 2 = 0", "demo-delete")
        remaining = self.db.execute(f"SELECT count(*) FROM {table}").scalar()
        historical = self.db.execute(f"SELECT count(*) FROM {table} FOR VERSION AS OF {snapshot}",
                                     "time-travel-read").scalar()
        self.db.execute(f"ALTER TABLE {table} EXECUTE rollback_to_snapshot({snapshot})", "snapshot-rollback")
        restored = self.db.execute(f"SELECT count(*) FROM {table}").scalar()
        diff = self.db.execute(
            f"SELECT count(*) FROM ((SELECT orderkey, linenumber, discounted_revenue AS net_revenue "
            f"FROM {table}) EXCEPT ALL (SELECT orderkey, linenumber, net_revenue "
            "FROM lakehouse.silver.shipping_lines))").scalar()
        if not (remaining < before["rows"] == historical == restored) or diff:
            raise AssertionError("Не удалось восстановление снапшота")
        evidence = {"before_rows": before["rows"], "after_delete_rows": remaining,
                    "historical_rows": historical, "restored_rows": restored, "snapshot_id": snapshot,
                    "data_files_unchanged_after_schema_evolution": files_before == files_after,
                    "new_column_null_rows": nulls, "renamed_column_sample": sample,
                    "before_ddl": before["ddl"], "after_ddl": after["ddl"],
                    "restored_minus_silver": diff}
        self.save("iceberg-demo.json", evidence)
        (self.directory / "time-travel.sql").write_text(
            f"-- Snapshot from this run, read without changing the table\n"
            f"SELECT count(*) FROM {table} FOR VERSION AS OF {snapshot};\n"
            f"-- Restore the existing demo table to that snapshot\n"
            f"ALTER TABLE {table} EXECUTE rollback_to_snapshot({snapshot});\n", encoding="utf-8")

    def bi(self):
        from metabase import demonstrate
        say("Metabase: подключение Trino, чтение Bronze и сохранение вопросов")
        self.save("bi.json", demonstrate(self.directory, self.db))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("all", "init", "elt", "benchmark", "iceberg", "bi", "verify"))
    parser.add_argument("--scale", default="sf1")
    parser.add_argument("--write-repeats", type=int, default=3)
    parser.add_argument("--query-repeats", type=int, default=5)
    args = parser.parse_args()
    if not re.fullmatch(r"sf(?:[1-9][0-9]*|0\.[0-9]+)", args.scale):
        parser.error("Масштаб задаётся как sf1, sf10, sf0.01")
    if args.write_repeats < 1 or args.query_repeats < 2:
        parser.error("Нужны минимум 1 замер записи и 2 замера запроса")
    RESULTS.mkdir(exist_ok=True)
    lock = RESULTS / ".run.lock"
    try:
        handle = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise SystemExit("Уже запущен другой процесс (results/.run.lock). Не запускайте два бенчмарка одновременно.")
    with os.fdopen(handle, "w") as stream:
        stream.write(str(os.getpid()))
    try:
        run_locked(args)
    finally:
        lock.unlink(missing_ok=True)


def run_locked(args):
    if args.command in ("all", "init"):
        directory = RESULTS / "runs" / now().replace(":", "").replace("-", "").replace(".", "_")
        directory.mkdir(parents=True)
        metadata = {"started_at": now(), "status": "running", "scale": args.scale,
                    "write_repeats": args.write_repeats, "query_repeats": args.query_repeats,
                    "seed": SEED, "matrix": MATRIX, "completed_stages": []}
        metadata["trino_jvm_options"] = (ROOT / "trino" / "jvm.config").read_text().splitlines()
        metadata["trino_properties"] = dict(
            line.split("=", 1) for line in (ROOT / "trino" / "config.properties").read_text().splitlines()
            if "=" in line and not line.lstrip().startswith("#"))
        for name in ("environment.json", "docker-ps.txt", "image-sources.json"):
            if (RESULTS / name).exists():
                shutil.copy2(RESULTS / name, directory / name)
        write_json(RESULTS / "latest.json", {"run": str(directory.relative_to(ROOT))})
    else:
        directory = ROOT / json.loads((RESULTS / "latest.json").read_text())["run"]
        metadata = json.loads((directory / "metadata.json").read_text())
        args.scale = metadata["scale"]
        args.write_repeats = metadata["write_repeats"]
        args.query_repeats = metadata["query_repeats"]
    pipe = Pipeline(directory, args)
    pipe.save("metadata.json", metadata)
    try:
        commands = ["init", "elt", "benchmark", "iceberg", "bi", "verify"] if args.command == "all" else [args.command]
        for command in commands:
            getattr(pipe, command)()
            if command not in metadata["completed_stages"]:
                metadata["completed_stages"].append(command)
            pipe.save("metadata.json", metadata)
        required = {"init", "elt", "benchmark", "iceberg", "bi", "verify"}
        metadata["status"] = "complete" if required <= set(metadata["completed_stages"]) else "partial"
        metadata.pop("error", None)
        metadata["updated_at"] = now()
        pipe.save("metadata.json", metadata)
        say("Готово. Результаты: " + str(directory.relative_to(ROOT)))
    except Exception as exc:
        metadata.update(status="failed", error=str(exc), updated_at=now())
        pipe.save("metadata.json", metadata)
        (directory / "error.log").write_text(traceback.format_exc(), encoding="utf-8")
        say(f"Ошибка: {exc}. Подробности: {directory.relative_to(ROOT)}/error.log")
        raise
if __name__ == "__main__":
    main()
