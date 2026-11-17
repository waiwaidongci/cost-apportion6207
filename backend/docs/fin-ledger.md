# 费用批次补传引擎（fin-ledger）

财务补传费用不再逐条覆盖：以**批次**为单位接收，全部校验通过后**原子切换**底稿版本，
核算在接收期间始终读到完整旧版。引擎基于 SQLite（WAL）+ Python 标准库实现，
无外部服务依赖，可独立运行，也可嵌入现有 FastAPI 后端。

## 1. 需求与实现对照

| # | 需求 | 实现机制 | 验证用例 |
|---|------|----------|----------|
| 1 | 批次接口，按来源+凭证号识别 | `open_batch / append_chunk / commit_batch`；识别键 `(source, voucher_no)` | test_semantics 02/15 |
| 2 | 乱序记录取最高修订 | 合并 SQL `ROW_NUMBER() OVER (PARTITION BY 键 ORDER BY revision DESC)` | test_semantics 02 |
| 3 | 凭证撤销留住修订号 | `is_cancel` 墓碑行，带 revision 永久保留，不做物理删除 | test_semantics 03/14 |
| 4 | 旧记录晚到仍作废 | UPSERT 的 `WHERE excluded.revision > expenses.revision`，低修订不覆盖 | test_semantics 04 |
| 5 | 修订相同内容冲突→拒收全批 | 批内/跨版两处 content_hash 冲突检查，命中即回滚候选版、批次 `rejected` | test_semantics 05/06 |
| 6 | 十万条分批落盘，额外内存 ≤128MB | 每块独立小事务 + 生成器流式处理；`cache_size=-8000`、`mmap_size=0`、`temp_store=FILE` | test_large_scale 01（实测峰值 ~35MB） |
| 7 | 中断后断点续跑不重复入账 | `(batch_id, seq)` 唯一；重传走幂等分支并校验指纹 | test_fault_injection 01、test_large_scale 02 |
| 8 | 接收期间核算读完整旧版 | 候选版是独立 `building` 版本；`current_version` 指针切换前不变 | test_fault_injection 02/03/04 |
| 9 | 校验完才原子切换 | 复制基线→冲突检查→合并→版本翻转→指针更新，**单个 IMMEDIATE 事务** | 全故障注入用例 |
| 10 | 并发批次重叠凭证按修订合并 | WAL + IMMEDIATE 写锁串行化提交；后提交者基于最新 current rebase | test_concurrency 01 |
| 11 | 无关更正不丢失 | 每批只动自己包含的键；UPSERT 不影响其他凭证 | test_concurrency 01/02 |
| 12 | 切换崩溃同成同败 | 单事务原子性：未提交随进程死亡回滚；已提交则完整可见。`recover()` 清理孤儿 building | test_fault_injection 03/04/06 |
| 13 | 排队核算固定完整新旧一版 | 入队时 `accounting_pins` 固定 version_id，之后切换不影响该任务 | test_semantics 12 |
| 14 | 保留原报告 | 报告带 version_id 永久留存，旧版报告不被覆盖 | test_semantics 12、CLI 演示 |

## 2. 数据模型（SQLite）

- `versions`：版本链，state ∈ building/current/retired，全局唯一 current。
- `current_version`：单行指针表，与版本翻转在同一事务更新。
- `expenses`：版本化底稿，`UNIQUE(version_id, source, voucher_no)`；
  墓碑（is_cancel=1）与普通行同构，修订号随墓碑保留。
- `batches`：批次状态机 receiving → committing → committed / rejected / failed，
  含 `checkpoint_seq`（已持久化最大连续序号）。
- `batch_records`：批次明细，`UNIQUE(batch_id, seq)` 保证重传不重复。
- `accounting_pins`：核算任务 → 固定版本。
- `reports` / `audits`：按版本留存的报告与审计流。

## 3. 关键流程

### 3.1 接收（receiving）
1. `open_batch` 记录基线版本 `base_version_id`（即此刻 current）。
2. `append_chunk` 在小事务内逐行校验、规范化（content_hash = SHA256(规范化载荷)），
   按 `(batch_id, seq)` 幂等插入；序号必须连续，断层直接拒绝。
3. 每块提交即 fsync（`synchronous=FULL`），崩溃只丢未提交的当前块。

### 3.2 提交（单事务原子切换）
1. 建 building 候选版，**完整复制基线**（新版本永远是完整一版，不是增量补丁）。
2. 冲突检查：批内同 (键,rev) 多指纹、与基线同 rev 异指纹 → 全批拒收。
3. 合并：批内每键最高修订，仅当严格高于基线修订才 UPSERT 覆盖。
4. 旧 current 置 retired、building 置 current、更新指针、批次置 committed，全部同事务。

### 3.3 崩溃恢复（recover）
- 未提交事务由 SQLite 原子回滚，磁盘上不存在半成品。
- 已落盘的孤儿 building 版本（极端场景）连同费用行删除。
- 卡在 committing/failed 的批次回到 receiving，明细不丢，重新提交即可。

### 3.4 核算
- `enqueue` 在当前 current 上打 pin；`run_job` 流式读固定版本并写报告。
- 切换后旧任务仍读旧版、旧报告永久可查；新任务固定新版。

## 4. 内存预算（128MB）

- 记录生成器 + 1000 条/块小事务，Python 侧常驻只有当前块；
- SQLite 页缓存限制 8MB，关闭 mmap，排序/临时结果落盘（`temp_store=FILE`）；
- 十万条实测进程峰值 RSS 约 35MB（见 `tests/test_large_scale.py`，用 /proc 采样）。

## 5. 使用方式

### CLI
```sh
python -m src.ledger.cli --db ./fin.db init
python -m src.ledger.cli --db ./fin.db open --expected 100000
python -m src.ledger.cli --db ./fin.db upload --batch 1 --file records.jsonl --chunk-size 1000
python -m src.ledger.cli --db ./fin.db commit --batch 1
python -m src.ledger.cli --db ./fin.db status | totals | expenses | reports
python -m src.ledger.cli --db ./fin.db enqueue --job J1 && \
python -m src.ledger.cli --db ./fin.db run-job --job J1
python -m src.ledger.cli --db ./fin.db recover
# 故障注入：FINFAULT=after_chunk:3 / commit_pre_merge / commit_post_build / commit_post_switch
```

### HTTP（标准库 ThreadingHTTPServer，默认 127.0.0.1:19350）
```sh
python -m src.ledger.server --db ./fin.db --port 19350
```
路由：`POST /batches`、`POST /batches/{id}/chunks`、`POST /batches/{id}/commit`、
`POST /recover`、`GET /versions`、`GET /versions/{id}/expenses`（NDJSON 流式）、
`POST /accounting/enqueue`、`POST /accounting/run`、`GET /reports`、`GET /audits`。

### 嵌入
```python
from src.ledger.store import LedgerStore
with LedgerStore("./fin.db") as s:
    bid = s.open_batch(expected_count=len(records))
    s.append_stream(bid, iter(records))
    s.commit_batch(bid)
```

## 6. 故障注入点

| FINFAULT | 位置 | 模拟效果 |
|----------|------|----------|
| `after_chunk:<n>` | 第 n 块落盘后 | 上传中途硬崩溃（os._exit(86)） |
| `commit_pre_merge` | 提交事务开始前 | 批次保持 receiving，旧版完好 |
| `commit_post_build` | 候选版构建后、翻转前 | 事务回滚，无半成品落盘 |
| `commit_post_switch` | 翻转语句后、COMMIT 前 | 整体回滚，对外仍是旧版 |

## 7. 测试

```sh
python -m unittest discover -s tests -v
```
30 个用例，覆盖核心语义（15）、进程级故障注入与恢复（6）、
线程与 HTTP 双层并发（5）、十万条规模/内存/续跑（4）。
