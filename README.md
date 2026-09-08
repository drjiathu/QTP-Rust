# QTP-Rust

使用 Rust 恢复沪深股票与 ETF 订单簿。当前提供完整单日通联／Datayes Parquet 的流式
逐笔回放、固定间隔截面和独立 raw snapshot 验证。库调用者也可直接应用标准 BookEvent。

“实时”指逐事件更新盘口；当前生产入口是离线文件回放，不包含交易所在线行情接入。

## 快速开始

rustup 按 `rust-toolchain.toml` 选择 Rust 1.85.1：

```bash
cargo build --release --locked --bin qtp-replay
target/release/qtp-replay replay \
  --date 20260828 --market SZ --symbols 000001,300750 \
  --snapshot-interval 30s --output-root output/example
target/release/qtp-replay validate \
  --date 20260828 --market SZ --symbols 000001,300750 \
  --report reports/manual-example.json
```

默认数据根目录为 `/hdd/data/stock/raw_level2_parquet`；输入布局和全市场／ETF 参数见
[使用手册](docs/Guidance.md)。生产截面严格包含 `quote_time < T` 的成功事件，
验证候选窗口不改变它；参考源是 raw snapshot，不是 canonical snapshot。

## 验证基线与边界

包版本为 `0.1.0`，历史全量验证的代码提交为 `73adb97`。两批同一冻结二进制覆盖 15 个日期、
30 个沪深任务；全部可比股票／ETF snapshot 匹配，停牌排除单列。
接口退役后的独立二进制另完成 20260828 两市全量重跑，26,224,341 个可比项全部匹配，
23 个状态排除项单列；不与旧版本统计混加。版本指纹、范围、结果和耗时见
[验证基线](docs/real-data-validation.md)。

深市 CLI 默认 RestAtLastTradePrice：市价单先隐藏，按自身实际成交价更新隐藏余量，
不再穿越时入簿，入簿后不重定价。限价单直接入簿，本方最优一次定价。
快照匹配不证明市价单每个中间态或严格待决路径精确；
Rust 枚举默认 RequireEvidence，与 CLI 默认不同，见[实现说明](docs/order-book-implementation.md)。

## 价格单位

生产价格乘数固定 `10_000`，一个单位为 `0.0001 元`。这是存储精度，不是证券最小报价
单位：普通股票限价和成交价通常到分，ETF 通常到厘，但深市原始委托为 Decimal(38,4)。

已抽样数据中，非零第四位小数均来自 `OrdType='1'` 市价委托的价格边界／参考价格：
由参考价格按买入 ×101%、卖出 ×99% 形成，例如 `12.89 ×101% =13.0189`、
`6.83 ×99% =6.7617`，也可能为零。这不是普通限价或实际成交价，不能用它将市价单入簿；
这是样本核验结论，不推广为所有来源的统一保护价公式。

因此保留四位精度，不降为 100 或 1000。生产逐笔及 raw snapshot 的 Decimal128
直接整数转换，不经 Float64；成交额以 `u128` 累计并检查溢出。旧 QTP f64 输入接口已移除。

## 文档与代码

旧 QTP legacy 公共接口和 C++ oracle 已移除；生产回放与共享核心保留，
固定 golden 和 Rust 回归测试继续维护，无需 C++ 工具链。
这是未发布的破坏性 API 变更；接口退役后验收单独存证，不自动继承历史十五日覆盖。

[文档索引](docs/README.md)分为四份正文：使用手册、实现说明、验收规范、验证基线。
[验收规范](docs/snapshot-validation-rules.md)是匹配规则的唯一来源，PDF 与历史归档仅作参考。

```text
src/market_data/       共享标量、核心强类型事件
src/order_book/        价位、FIFO、失败原子性、统计与只读查询
src/production/        Parquet、通道修复／回放、截面与验证
src/bin/qtp-replay.rs  CLI 入口
tests/                单元外的集成、属性、固定 golden 回归
examples/             回放与验证性能测量
reports/              冻结验证证据
analysis/             当前分析入口
```

开发检查和贡献流程见 [CONTRIBUTING](CONTRIBUTING.md)，变化记录见 [CHANGELOG](CHANGELOG.md)，
安全问题按 [SECURITY](SECURITY.md) 报告。许可证为 [LGPL-3.0-only](LICENSE)，
组合条款见 [COPYING](COPYING)、[COPYING.LESSER](COPYING.LESSER)，来源见 [NOTICE](NOTICE)。
