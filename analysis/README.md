# 当前分析工具

保留最新全量回归入口、它所需的共享驱动以及通用诊断工具。
这些脚本不属于生产 API，运行需要本机原始数据和独立保存的历史证据。

| 文件 | 当前用途 |
| --- | --- |
| `run_borrowed_lookup_validation.py` | 最近一轮十五日全量回归入口；核对输入身份、语义报告、四项候选计数及耗时 |
| `run_callback_validation.py` | 上述入口依赖的 ABBA／全量比较封装 |
| `run_p0_optimized_regression.py` | 比较报告、冻结源码及汇总的共享驱动 |
| `run_current_full_regression.py` | 读取输入清单、执行任务、验收报告及汇总的底层工具 |
| `extract_validation_symbols.py` | 从验证报告提取不匹配证券代码 |

后三个驱动中的历史命名与批次常量保留，避免为目录清理引入执行逻辑改动。
依赖链为 `borrowed_lookup → callback → p0 → current`，不能只按文件名或日期删除。

## 全量回归

先构建正常版本，不要使用历史试验二进制：

```bash
cargo build --release --locked --features profiling --example validation_benchmark
/home/jxw06/workspace/proj/clara/.venv/bin/python analysis/run_borrowed_lookup_validation.py
```

该入口用于复现固定批次，基线为
`reports/20260909-p1-allocation-full-regression/`，输出为
`reports/20260909-p1-borrowed-lookup-full-regression/`，已有输出时拒绝覆盖。
本机该批次已完成，因此直接重跑会被拒绝；新批次须先指定独立输出目录和适当基线。
导入共享驱动还依赖历史十日／五日 manifest，不是克隆后即可直接运行的通用 CLI。
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
当前验收范围与计时结果见[验证基线](../docs/real-data-validation.md)。

## 已结束的工具与恢复

2026-09-10 清理了旧五日扩展、旧 P1 回归、allocation 阶段试验、三项 P1 可行性试验入口，
以及两个旧 notebook 和 Python 缓存。没有删除原始行情、运行报告、实验源码或冻结二进制。

清理前的完整 `analysis/`、`docs/` 位于
`reports/20260910-analysis-docs-cleanup/`，包括尚未提交的可行性试验脚本；
该备份不随 Git 分发。已提交旧文件也可从 `33bf3bb` 恢复。
早期、更旧的工具见 Git 提交 `f3f77c0`（历史整理前 `73adb97`）。

需要重现历史试验时，从备份或对应批次的冻结源码恢复配套文件到原路径布局，
不要原地改写旧报告／源码快照。三项可行性原型没有合入生产代码，也不再作为待实施计划。
