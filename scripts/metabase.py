"""Demonstrate a separate BI client against Bronze through the Metabase API."""
from __future__ import annotations

import json
import os
from pathlib import Path
import secrets
import time

from client import http_json, now, wait_http, write_json

BASE = "http://localhost:3000"
ROOT = Path(__file__).resolve().parent.parent
LOCAL_CREDS = ROOT / "results" / "metabase-credentials.local.json"
PENDING_CREDS = ROOT / "results" / "metabase-credentials.pending.local.json"


def authenticate(properties: dict) -> dict:
    # setup-token can remain nonempty after setup in this version. The explicit
    # has-user-setup flag determines whether it is appropriate to create a user.
    if properties.get("has-user-setup", True) is False:
        token = properties.get("setup-token")
        if not token:
            raise RuntimeError("Metabase не выдал токен первоначальной настройки")
        creds = (json.loads(PENDING_CREDS.read_text()) if PENDING_CREDS.exists() else
                 {"username": "andrey@example.com", "password": "Pr1!" + secrets.token_urlsafe(24)})
        # Retain the candidate if the server commits setup but the HTTP response
        # is lost. An unsuccessful setup must never overwrite known credentials.
        write_json(PENDING_CREDS, creds)
        PENDING_CREDS.chmod(0o600)
        session = http_json(BASE + "/api/setup", "POST", {
            "token": token,
            "prefs": {"site_name": "Lakehouse — Андрей Обрезков", "allow_tracking": False},
            "user": {"first_name": "Андрей", "last_name": "Обрезков",
                     "email": creds["username"], "password": creds["password"]},
        })
        write_json(LOCAL_CREDS, creds)
        LOCAL_CREDS.chmod(0o600)
        PENDING_CREDS.unlink(missing_ok=True)
        return session
    if os.environ.get("MB_USER") and os.environ.get("MB_PASSWORD"):
        return http_json(BASE + "/api/session", "POST", {
            "username": os.environ["MB_USER"], "password": os.environ["MB_PASSWORD"]})
    last_error = None
    for path in (PENDING_CREDS, LOCAL_CREDS):
        if not path.exists():
            continue
        creds = json.loads(path.read_text())
        try:
            session = http_json(BASE + "/api/session", "POST", creds)
        except RuntimeError as exc:
            if getattr(exc.__cause__, "code", None) not in (400, 401):
                raise
            last_error = exc
            continue
        if path == PENDING_CREDS:
            write_json(LOCAL_CREDS, creds)
            LOCAL_CREDS.chmod(0o600)
            PENDING_CREDS.unlink(missing_ok=True)
        return session
    raise RuntimeError("Metabase уже настроен. Проверьте локальный пароль или задайте MB_USER и MB_PASSWORD.") from last_error


def demonstrate(directory: Path, trino) -> dict:
    wait_http(BASE + "/api/health")
    properties = http_json(BASE + "/api/session/properties")
    session = authenticate(properties)
    headers = {"X-Metabase-Session": session["id"]}

    def api(path, method="GET", body=None):
        return http_json(BASE + "/api" + path, method, body, headers, timeout=180)

    try:
        listing = api("/database")
        databases = listing.get("data", []) if isinstance(listing, dict) else listing
        database = next((d for d in databases if d["name"] == "Local Lakehouse"), None)
        if database is None:
            database = api("/database", "POST", {
                "name": "Local Lakehouse", "engine": "starburst",
                "details": {"host": "trino", "port": 8080, "catalog": "lakehouse",
                            "user": "student", "password": "", "ssl": False,
                            "tunnel-enabled": False},
                "is_full_sync": False, "is_on_demand": True,
            })
        database_id = database["id"]

        def dataset(sql):
            return {"database": database_id, "type": "native",
                    "native": {"query": sql, "template-tags": {}}, "parameters": []}

        bronze_count_sql = "SELECT count(*) AS row_count FROM lakehouse.bronze.lineitem"
        years_sql = (ROOT / "sql" / "bronze_orders_by_year.sql").read_text()
        gold_sql = "SELECT * FROM lakehouse.gold.shipping_priority ORDER BY revenue DESC, orderkey"
        evidence = {"at": now(), "database_id": database_id,
                    "connection": {"driver": "starburst", "host": "trino", "port": 8080,
                                   "catalog": "lakehouse"}, "queries": []}
        for sql in (bronze_count_sql, years_sql, gold_sql):
            response = api("/dataset", "POST", dataset(sql))
            if response.get("status") != "completed":
                raise RuntimeError("Запрос Metabase не выполнен: " + str(response.get("error", response.get("status"))))
            data = response["data"]
            evidence["queries"].append({"sql": sql, "status": response["status"],
                                        "columns": [c["name"] for c in data["cols"]],
                                        "rows": data["rows"], "running_time_ms": response.get("running_time")})
        expected = trino.execute("SELECT count(*) FROM lakehouse.bronze.lineitem").scalar()
        actual = evidence["queries"][0]["rows"][0][0]
        if expected != actual:
            raise AssertionError("Metabase читает другое число строк Bronze")
        expected_orders = trino.execute("SELECT count(*) FROM lakehouse.bronze.orders").scalar()
        if sum(row[1] for row in evidence["queries"][1]["rows"]) != expected_orders:
            raise AssertionError("Агрегация Bronze в Metabase не совпала с Trino")
        if len(evidence["queries"][2]["rows"]) != 10:
            raise AssertionError("Metabase не получил 10 строк Gold")

        # Keep repeat runs idempotent: update our saved questions if they already exist.
        # H2 can fail on the first allocation of a personal collection id. Retry
        # only this GET on a transient 500; persistent errors remain failures.
        for attempt in range(3):
            try:
                existing = api("/card")
                break
            except RuntimeError as exc:
                if attempt == 2 or getattr(exc.__cause__, "code", None) != 500:
                    raise
                time.sleep(1)
        cards = existing.get("data", []) if isinstance(existing, dict) else existing
        saved = []
        for name, sql, display, settings in (
            ("Bronze — заказы по годам", years_sql, "bar",
             {"graph.dimensions": ["order_year"], "graph.metrics": ["orders_count"]}),
            ("Gold — Q3 Shipping Priority", gold_sql, "table", {}),
        ):
            body = {"name": name, "description": "Практическая 1. Андрей Обрезков. Вариант 1 (Q3).",
                    "dataset_query": dataset(sql), "display": display, "visualization_settings": settings}
            old = next((c for c in cards if c["name"] == name), None)
            card = api("/card" + (f"/{old['id']}" if old else ""), "PUT" if old else "POST", body)
            saved.append({"name": name, "id": card["id"], "url": BASE + f"/question/{card['id']}"})
        evidence["saved_questions"] = saved
        evidence["bronze_lineitem_rows_equal_trino"] = True
        evidence["bronze_year_totals_equal_orders"] = True
        write_json(directory / "bi-bronze-queries.json", evidence["queries"])
        return evidence
    finally:
        # Session ids and passwords never go into published run artifacts.
        http_json(BASE + "/api/session", "DELETE", {"session_id": session["id"]}, headers)
