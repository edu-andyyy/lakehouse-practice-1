"""Small standard-library client for Trino's statement REST protocol."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def http_json(url: str, method: str = "GET", payload: Any = None,
              headers: dict | None = None, timeout: int = 60) -> Any:
    data = None
    request_headers = dict(headers or {})
    if payload is not None:
        if isinstance(payload, str):
            data = payload.encode()
            request_headers.setdefault("Content-Type", "text/plain; charset=utf-8")
        else:
            data = json.dumps(payload).encode()
            request_headers.setdefault("Content-Type", "application/json")
    request = Request(url, data=data, method=method, headers=request_headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            content = response.read()
            return json.loads(content) if content else None
    except HTTPError as exc:
        # Response bodies can contain database credentials: do not log them.
        raise RuntimeError(f"HTTP {exc.code}: {method} {url}") from exc


def wait_http(url: str, seconds: int = 600) -> dict:
    deadline = time.monotonic() + seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            response = http_json(url, timeout=5)
            if isinstance(response, dict) and response.get("starting") is True:
                time.sleep(2)
                continue
            return response
        except (OSError, URLError, RuntimeError) as exc:
            last_error = str(exc)
            time.sleep(2)
    raise RuntimeError(f"Сервис не готов: {url}. {last_error}")


@dataclass
class Query:
    query_id: str
    columns: list[str]
    rows: list[list]
    stats: dict
    client_wall_ms: float

    def records(self) -> list[dict]:
        return [dict(zip(self.columns, row)) for row in self.rows]

    def scalar(self):
        if len(self.rows) != 1 or len(self.columns) != 1:
            raise ValueError("Ожидалась одна строка с одной колонкой")
        return self.rows[0][0]

    @property
    def elapsed_ms(self) -> int:
        return self.stats["elapsedTimeMillis"]


class Trino:
    def __init__(self, log: Path, url: str = "http://localhost:8080"):
        self.url = url.rstrip("/")
        self.log = log
        self.log.parent.mkdir(parents=True, exist_ok=True)
        self.headers = {"X-Trino-User": "andrey-obrezkov", "X-Trino-Source": "practice1"}

    def execute(self, sql: str, label: str = "sql") -> Query:
        sql = sql.strip().rstrip(";")
        started = now()
        start = time.perf_counter()
        page = http_json(self.url + "/v1/statement", "POST", sql, self.headers)
        query_id = page["id"]
        rows, columns = [], []
        while True:
            if "columns" in page:
                columns = [column["name"] for column in page["columns"]]
            rows.extend(page.get("data", []))
            if "error" in page:
                error = page["error"]
                self._log({"at": started, "label": label, "query_id": query_id,
                           "sql": sql, "error": error, "stats": page.get("stats", {})})
                raise RuntimeError(f"{query_id}: {error.get('errorName')}: {error.get('message')}")
            if not page.get("nextUri"):
                break
            if time.perf_counter() - start > 1800:
                http_json(page["nextUri"], "DELETE", headers=self.headers)
                raise TimeoutError(f"Query timeout: {query_id}")
            page = http_json(page["nextUri"], headers=self.headers)
        elapsed = (time.perf_counter() - start) * 1000
        query = Query(query_id, columns, rows, page["stats"], elapsed)
        self._log({"at": started, "label": label, "query_id": query_id, "sql": sql,
                   "stats": query.stats, "client_wall_ms": round(elapsed, 3),
                   "result_row_count": len(rows)})
        return query

    def _log(self, record: dict) -> None:
        with self.log.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")


def split_sql(text: str) -> list[str]:
    """Split our SQL scripts, preserving semicolons in quoted literals/comments."""
    statements, current = [], []
    quote, line_comment, block_comment = None, False, False
    i = 0
    while i < len(text):
        char, pair = text[i], text[i:i + 2]
        if line_comment:
            if char == "\n":
                line_comment = False
                current.append(char)
        elif block_comment:
            if pair == "*/":
                block_comment = False
                current.append(" ")
                i += 1
        elif quote:
            current.append(char)
            if char == quote:
                if i + 1 < len(text) and text[i + 1] == quote:
                    current.append(quote)
                    i += 1
                else:
                    quote = None
        elif pair == "--":
            line_comment = True
            i += 1
        elif pair == "/*":
            block_comment = True
            i += 1
        elif char in "'\"":
            quote = char
            current.append(char)
        elif char == ";":
            if "".join(current).strip():
                statements.append("".join(current).strip())
            current = []
        else:
            current.append(char)
        i += 1
    if quote or block_comment:
        raise ValueError("Незавершённая строка или комментарий SQL")
    if "".join(current).strip():
        statements.append("".join(current).strip())
    return statements
