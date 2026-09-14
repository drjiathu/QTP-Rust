# 当前分析工具

仅保留可复用的全日期验证入口、配套测试和通用诊断工具。
这些脚本不属于生产 API；真实数据验证需要本机原始数据和事先生成的输入清单。

| 文件 | 当前用途 |
| --- | --- |
| `run_full_market_validation.py` | 使用冻结输入清单执行全日期、沪深股票/ETF验证；支持六进程资源准入、批次锁和任务级恢复 |
| `test_full_market_validation.py` | 外层调度器的计数、资源门槛及收据测试，不运行真实行情 |
| `extract_validation_symbols.py` | 从验证报告提取不匹配证券代码 |

全日期验证器独立运行，不依赖已归档的优化实验调用链或历史十五日 manifest。

## 全量回归

### 全日期验证

`run_full_market_validation.py` 不依赖历史十五日报告。先只读生成输入清单，包含
每个文件的路径、日期、feed、行数、字节数、`mtime_ns`、字段 Schema SHA256 和状态，
再使用同一版本冻结的 profiling 程序启动新批次。清单示例仅本地保存在
`reports/20260910-full-validation-plan/inventory.json`，不随克隆提供。

```bash
cargo build --release --locked --features profiling --example validation_benchmark
/home/jxw06/workspace/proj/clara/.venv/bin/python \
  analysis/run_full_market_validation.py \
  --inventory reports/<inventory>/inventory.json \
  --binary target/release/examples/validation_benchmark \
  --output reports/<new-run-id> \
  --spool-root /ssd/qtp-validation-spool/<new-run-id> \
  --source-commit <tested-commit> --workers 6
```

省略标的参数表示全部支持股票/ETF；不写定时截面、不扩窗、不保留成功逐帧明细，
失败明细默认上限 5000，汇总不受截断影响。v2 只比较真实选中帧，未选中原因单独审计；
零有效参考是无比较运行，不是验收通过。默认试运行日期为
20260114、20260717、20260828，可通过 `--pilot-dates` 调整。
`summary.json` 是进度及计数摘要，`manifest.json` 记录任务和资源快照。汇总按市场和
报告版本分别保存 `markets.<market>.versions.<version>`，不混合 v1 虚拟锚点与 v2 真实参考。

恢复同一批次须使用相同脚本、程序、输入和参数，并传入 `--resume`；不得与旧控制器
同时启动。仍存活的旧任务不会重复执行，失败结果保留且不自动重试。
子任务运行需要实际临时盘写权限；资源不足会暂停派发，不会终止其他进程。
长任务应由 systemd 等持久化进程管理器托管，不能将临时工具会话视为可靠后台服务。
需要遇错即停时添加 `--stop-on-failure`：首个市场日任务产生不匹配、输入错误或运行错误
后停止派发，并终止本批次启动的其余进程组；取消任务记为 `cancelled_after_failure`，
不视为验证通过或失败。报告和临时分片保留，未运行任务仍为 pending。该选项不改变
单个市场日内部的逐帧比较流程；未选中有效参考不是停止条件。
核验程序返回成功不等于严格市价策略的 `is_standard_acceptance()`。

```bash
/home/jxw06/workspace/proj/clara/.venv/bin/python \
  -m unittest discover -s analysis -p test_full_market_validation.py
```

日常回放与单日验证使用[使用手册](../docs/使用手册.md)中的生产入口。

需要 PyArrow 的脚本使用 Clara Python。输入清单核对路径、行数、Schema、大小和修改时间，
不声称进行全文件内容哈希核验。成功明细省略时，报告相等不等于逐帧成功候选元数据相等；
后者由核心差分测试补充覆盖。六进程 wall time 不等于独占 CPU 基准。

## 诊断与只读检查

```bash
python3 analysis/extract_validation_symbols.py reports/example-validation.json
ruff check analysis
ruff format --check analysis
```

详细报告、收据、输入清单与源码快照均保存在本地 `reports/`，由 Git 忽略。
当前验收范围与计时结果仅在本地 `reports/real-data-validation.md` 留存，不随仓库提供。
通用测量方法见[使用手册](../docs/使用手册.md#profiling)。

## 已结束的工具与恢复

2026-09-14 归档旧优化实验链：`run_borrowed_lookup_validation.py` →
`run_callback_validation.py` → `run_p0_optimized_regression.py` →
`run_current_full_regression.py`，并移除 Python 缓存。它们绑定已经完成的固定批次，
不再作为当前验证入口；未改变 Rust 恢复或比较逻辑。

清理前完整 `analysis/`（含未提交修改及配套新验证器）保存在
`reports/20260914-analysis-cleanup/analysis/`。已提交版本也可从 `0c91b26` 恢复，
但 Git 版本不包含本次备份中的未提交修改。
2026-09-10 的更早清理备份仍在 `reports/20260910-analysis-docs-cleanup/`。

本地备份不随 Git 分发。需要重现历史实验时，从备份或对应批次的冻结源码恢复整套
依赖及原路径布局，不要覆盖已有结果。原始行情、运行报告和冻结程序均未删除。
