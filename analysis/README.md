# 当前保留的分析工具

保留最新批次入口和通用定位工具。依赖旧快照、临时提取文件或旧策略的一次性脚本及
notebook 已移出工作树；旧代码可从 Git 提交 `73adb97` 恢复。

| 文件 | 用途 |
| --- | --- |
| `run_current_full_regression.py` | 十日驱动及共享执行、报告审计、profiling 汇总函数 |
| `run_additional_random_validation.py` | 新增五日驱动，复用上述模块 |
| `20260907-current-full-regression.ipynb` | 读取十日结果、异常与计时 |
| `20260908-additional-random-validation.ipynb` | 重现五日随机选择并读取结果 |
| `extract_validation_symbols.py` | 从 pretty JSON 验证报告提取不匹配证券代码 |

批次日期、输出目录和版本检查固定用于重现已存证据，`--launch` 拒绝覆盖已有目录。
运行新批次应使用独立批次实现/目录，不覆盖旧结果。需要 PyArrow 的脚本使用 Clara Python。

读取/刷新现存汇总，不会重新回放：

```bash
/home/jxw06/workspace/proj/clara/.venv/bin/python analysis/run_current_full_regression.py --summarize
/home/jxw06/workspace/proj/clara/.venv/bin/python analysis/run_additional_random_validation.py --summarize
python3 analysis/extract_validation_symbols.py reports/20260908-additional-random-full/20260813-sz-full.json
```

正式证据见 [reports/README.md](../reports/README.md)。批次内 `source-snapshot/analysis`
是不可变运行版本，允许保留旧路径，不作为当前执行入口；不要为清理而改写源码快照。
