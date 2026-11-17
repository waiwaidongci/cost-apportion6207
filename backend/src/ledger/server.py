"""批次补传的 HTTP 接口（标准库实现，零外部依赖）。

线程模型：ThreadingHTTPServer，每个请求开一个 SQLite 连接（WAL 支持并发读 +
串行写）。写操作由数据库 IMMEDIATE 锁串行化，重叠凭证的并发批次按提交顺序 rebase。

路由：
POST   /batches                      开批        {expected_count?}
GET    /batches                      批次列表
GET    /batches/{id}                 批次状态
POST   /batches/{id}/chunks          追加一块     {first_seq?, records:[...]}
POST   /batches/{id}/commit          校验并原子切换
POST   /recover                      崩溃恢复
GET    /versions                     版本列表
GET    /versions/current             当前版本指针
GET    /versions/{id}/totals         版本汇总
GET    /versions/{id}/expenses       版本费用（流式 JSON 行）
POST   /accounting/enqueue           核算排队（固定版本）
POST   /accounting/run               执行排队任务 {job_id}
GET    /reports                      报告列表（旧版永久保留）
GET    /reports/{id}                 报告内容
GET    /audits                       审计流
GET    /health                       健康检查
"""
from __future__ import annotations

import json
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .db import connect, initialize
from .errors import LedgerError
from .store import LedgerStore

# 连接按线程缓存：同一 keep-alive 线程复用连接，关闭时由进程回收。
_tls = threading.local()


def _conn(db_path: str):
    if not hasattr(_tls, "conn") or _tls.path != db_path:
        conn = connect(db_path)
        initialize(conn)
        _tls.conn = conn
        _tls.path = db_path
    return _tls.conn


def make_handler(db_path: str):
    class Handler(BaseHTTPRequestHandler):
        server_version = "FinBatchLedger/1.0"

        def log_message(self, fmt, *args):  # 安静点，测试输出不被污染
            pass

        def _json(self, status: int, payload) -> None:
            data = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body(self) -> dict:
            length = int(self.headers.get("Content-Length", 0))
            if not length:
                return {}
            raw = self.rfile.read(length)
            try:
                return json.loads(raw)
            except json.JSONDecodeError as exc:
                raise LedgerError(f"请求体不是合法 JSON: {exc}")

        def _store(self) -> LedgerStore:
            # 轻量包装：复用线程连接，故障点仍由环境变量控制。
            s = LedgerStore.__new__(LedgerStore)
            s.path = db_path
            s.conn = _conn(db_path)
            s.chunk_size = 1000
            import os
            s._fault = os.environ.get("FINFAULT", "")
            return s

        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

        def _dispatch(self, method: str):
            try:
                path = urlparse(self.path).path.rstrip("/") or "/"
                store = self._store()
                self._route(method, path, store)
            except LedgerError as exc:
                self._json(409 if exc.code == "batch_rejected" else
                           404 if exc.code == "batch_not_found" else
                           422 if exc.code in ("batch_state", "record_invalid",
                                                "sequence_gap", "pin_error") else 400,
                           {"error": exc.code, "message": exc.message,
                            "details": exc.details})
            except Exception as exc:  # pragma: no cover
                self._json(500, {"error": "internal", "message": str(exc)})

        def _route(self, method: str, path: str, store: LedgerStore):
            if method == "GET" and path == "/health":
                store.conn.execute("SELECT 1").fetchone()
                return self._json(200, {"status": "ok"})

            if method == "POST" and path == "/recover":
                return self._json(200, store.recover())

            if method == "POST" and path == "/batches":
                body = self._body()
                bid = store.open_batch(expected_count=body.get("expected_count"))
                return self._json(201, {"batch_id": bid})

            if method == "GET" and path == "/batches":
                return self._json(200, {"batches": store.list_batches()})

            m = re.fullmatch(r"/batches/(\d+)", path)
            if method == "GET" and m:
                return self._json(200, store.get_batch(int(m.group(1))))

            m = re.fullmatch(r"/batches/(\d+)/chunks", path)
            if method == "POST" and m:
                body = self._body()
                result = store.append_chunk(
                    int(m.group(1)), body.get("records", []),
                    first_seq=body.get("first_seq"))
                return self._json(200, result)

            m = re.fullmatch(r"/batches/(\d+)/commit", path)
            if method == "POST" and m:
                result = store.commit_batch(int(m.group(1)))
                return self._json(200, result)

            if method == "GET" and path == "/versions":
                return self._json(200, {"versions": store.list_versions()})

            if method == "GET" and path == "/versions/current":
                return self._json(200, {"version_id": store.current_version_id()})

            m = re.fullmatch(r"/versions/(\d+)/totals", path)
            if method == "GET" and m:
                return self._json(200, store.version_totals(int(m.group(1))))

            m = re.fullmatch(r"/versions/(\d+)/expenses", path)
            if method == "GET" and m:
                # 流式：每行一个 JSON 对象（application/x-ndjson），避免整版进内存。
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
                self.end_headers()
                for row in store.iter_version(int(m.group(1))):
                    self.wfile.write(
                        (json.dumps(row, ensure_ascii=False, default=str) + "\n").encode())
                return

            if method == "POST" and path == "/accounting/enqueue":
                from . import accounting
                body = self._body()
                return self._json(201, accounting.enqueue(
                    store, job_id=body.get("job_id")))

            if method == "POST" and path == "/accounting/run":
                from . import accounting
                body = self._body()
                if "job_id" not in body:
                    raise LedgerError("缺少 job_id")
                return self._json(200, accounting.run_job(store, body["job_id"]))

            if method == "GET" and path == "/reports":
                return self._json(200, {"reports": store.list_reports()})

            m = re.fullmatch(r"/reports/(\d+)", path)
            if method == "GET" and m:
                return self._json(200, store.get_report(int(m.group(1))))

            if method == "GET" and path == "/audits":
                return self._json(200, {"audits": store.list_audits()})

            self._json(404, {"error": "not_found", "message": path})

    return Handler


def serve(db_path: str, host: str = "127.0.0.1", port: int = 19350) -> None:
    # 启动即做一次崩溃恢复，保证对外可用状态一致。
    with LedgerStore(db_path) as s:
        s.recover()
    httpd = ThreadingHTTPServer((host, port), make_handler(db_path))
    httpd.serve_forever()


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="费用批次补传引擎 HTTP 服务")
    parser.add_argument("--db", default=os.environ.get("FIN_LEDGER_DB", "./fin-ledger.db"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19350)
    args = parser.parse_args()
    print(f"fin-ledger listening on http://{args.host}:{args.port} (db={args.db})")
    serve(args.db, args.host, args.port)
