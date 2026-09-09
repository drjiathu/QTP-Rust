# 当前保留的分析工具

保留最新批次入口和通用定位工具。依赖旧快照、临时提取文件或旧策略的一次性脚本及
notebook 已清理；旧代码可从 Git 提交 `f3f77c0`（历史整理前 `73adb97`）恢复。
当前跟踪下列 Python 工具与本文档，不属于生产接口。

| 文件 | 用途 |
| --- | --- |
| `run_current_full_regression.py` | 十日驱动及共享执行、报告审计、profiling 汇总函数 |
| `run_additional_random_validation.py` | 新增五日驱动，复用上述模块 |
| `run_p0_optimized_regression.py` | P0 十五日回归，与历史两批逐份比较语义报告；提供优化回归共用驱动 |
| `run_p1_optimized_regression.py` | 三项 P1 优化的十五日回归，与冻结 P0 基线比较报告、分项耗时和峰值内存 |
| `run_callback_validation.py` | validation 回调优化：`abba` 串行样本对照、`full` 十五日全量回归，基线为 `72b7d43` |
| `extract_validation_symbols.py` | 从 pretty JSON 验证报告提取不匹配证券代码 |

批次日期、输出目录和版本检查固定用于重现已存证据，`--launch` 拒绝覆盖已有目录。
P0/P1 入口直接执行即启动，使用已构建的 release profiling example，目标目录已存在则拒绝运行。
运行新批次应使用独立批次实现/目录，不覆盖旧结果。需要 PyArrow 的脚本使用 Clara Python。

回调优化的运行顺序（先构建 release profiling example）：

```bash
cargo build --release --locked --features profiling --example validation_benchmark
/home/jxw06/workspace/proj/clara/.venv/bin/python analysis/run_callback_validation.py abba
/home/jxw06/workspace/proj/clara/.venv/bin/python analysis/run_callback_validation.py full
```

ABBA 在 20260828 使用沪市 `600519,510300`、深市 `000001,159915`，每个市场按旧、新、
新、旧串行运行；保存二进制指纹、命令、计时和报告一致性。小样本不能代替全市场验收。

`20260907-current-full-regression.ipynb` 和 `20260908-additional-random-validation.ipynb`
仅用于本地历史结果阅读，已停止跟踪但保留本地文件。`analysis/*.ipynb`、缓存及
整个 `reports/` 由 Git 忽略；克隆仓库不会获得这些文件。脚本需要的输入、报告和冻结
二进制必须单独准备，不因保留脚本就能在空环境复现历史结果。

读取/刷新现存汇总，不会重新回放：

```bash
/home/jxw06/workspace/proj/clara/.venv/bin/python analysis/run_current_full_regression.py --summarize
/home/jxw06/workspace/proj/clara/.venv/bin/python analysis/run_additional_random_validation.py --summarize
python3 analysis/extract_validation_symbols.py reports/20260908-additional-random-full/20260813-sz-full.json
```

已提交的结果摘要见[验证基线](../docs/real-data-validation.md)，详细证据位于本地 `reports/`。
优化回归的 `comparison.json`/`comparison.md` 比较同日同市场结果与时间。
语义报告比较包括汇总、阶段审计、排除项和失败明细；不包含已省略的成功逐帧候选元数据。
首次命中和最佳失败候选的保真另由差分单元测试覆盖。六进程并发耗时不能视为独占 CPU 基准。
批次内 `source-snapshot/analysis`
是不可变运行版本，允许保留旧路径，不作为当前执行入口；不要为清理而改写源码快照。

## 代码检查

```bash
ruff check analysis
ruff format --check analysis
```

只检查受维护的 Python 工具，不对本地 notebook 或冻结报告源码批量修复。
修复 lint 时保留非零子进程退出码审计；worker 异常必须写入失败收据并令批次验收失败。
