# 文档索引

正文按四个职责维护，避免规则、实现和实验结论在多个文件各自演化：

| 要解决的问题 | 文档 | 唯一维护内容 |
| --- | --- | --- |
| 如何编译、执行、调用库 | [使用手册](Guidance.md) | CLI、核心 Rust API 示例、输出、错误和报告阅读 |
| 代码如何恢复订单簿 | [实现说明](order-book-implementation.md) | 模块调用、沪深映射、市价策略、逆序修复、资源边界 |
| 什么结果算验证通过 | [验收规范](snapshot-validation-rules.md) | 阶段选帧、窗口、比较字段、特殊收盘视图、异常分类 |
| 哪个版本在哪些数据上验证过 | [验证基线与计时](real-data-validation.md) | 冻结版本、数据范围、证据链接和 profiling 口径 |

当前包版本仍为 `0.1.0`；旧 QTP 接口和 C++ oracle 已移除，固定 golden 与 Rust 回归测试保留。
历史十五日全量验证与接口退役后的 20260828 回归分别存证，提交与二进制指纹以验证基线为准。
文档整理不等同于创建发布 tag，也不扩大已验证范围。开发检查见
[CONTRIBUTING](../CONTRIBUTING.md#required-checks)，后续任务使用 Issue 跟踪。

## 原始参考资料

本目录的 PDF 原件保留，不合并、不改写。通联 V4.1 说明供应商字段；交易所接口文件
说明原生消息或申报；具体使用时须核对版本及适用日期，不以 PDF 文件名推定生效范围。
尤其本目录两份深交所 `v1.32` 文件名为**交易接口**，不能当作 Binary／STEP **行情接口**。

证据、运行收据和源码快照在 [reports/](../reports/README.md)；当前分析入口在
[analysis/](../analysis/README.md)。大规模逐帧报告不复制到 docs。

## 合并与历史归档

- 原 `order-book-replay-guide.md` 已合并；旧 QTP 接口退役后，使用手册改为核心 Rust API。
- 原 `sz-order-replay.md` 和使用手册中的内部顺序处理合入实现说明。
- 原 `validation-performance.md` 合入验证基线的“Profiling”。
- [C++ 迁移方案](archive/cpp-to-rust-migration-plan.md)、
  [早期 20260828 调查](archive/real-data-validation-20260828-investigation.md) 保留为历史，
  不再定义当前行为；归档正文、官方 PDF 和报告源码快照不因本次合并而重写。
