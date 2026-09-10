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
[使用手册](docs/使用手册.md)。生产截面严格包含 `quote_time < T` 的成功事件，
验证候选窗口不改变它；参考源是 raw snapshot，不是 canonical snapshot。

## 验证与适用边界

验收规则见[验收规范](docs/截面数据验证匹配规则.md)。具体版本的数据覆盖、结果及性能记录
仅在本地 `reports/` 留存，不随 Git 分发；耗时依赖硬件、输入及运行负载，不作为通用性能承诺。

深市 CLI 默认 RestAtLastTradePrice：市价单先隐藏，按自身实际成交价更新隐藏余量，
不再穿越时入簿，入簿后不重定价。限价单直接入簿，本方最优一次定价。
快照匹配不证明市价单每个中间态或严格待决路径精确；
Rust 枚举默认 RequireEvidence，与 CLI 默认不同，见[实现说明](docs/订单簿恢复实现说明.md)。

## 价格单位

生产价格乘数固定 `10_000`，一个单位为 `0.0001 元`。这是存储精度，不是证券最小报价
单位：普通股票限价和成交价通常到分，ETF 通常到厘，但深市原始委托为 Decimal(38,4)。

已抽样数据中，非零第四位小数均来自 `OrdType='1'` 市价委托的价格边界／参考价格：
由参考价格按买入 ×101%、卖出 ×99% 形成，例如 `12.89 ×101% =13.0189`、
`6.83 ×99% =6.7617`，也可能为零。这不是普通限价或实际成交价，不能用它将市价单入簿；
这是样本核验结论，不推广为所有来源的统一保护价公式。

因此保留四位精度，不降为 100 或 1000。生产逐笔及 raw snapshot 的 Decimal128
直接整数转换，不经 Float64；成交额以 `u128` 累计并检查溢出。

## 文档与开发

[文档索引](docs/文档索引.md)分为三份正文：使用手册、实现说明、验收规范。
[验收规范](docs/截面数据验证匹配规则.md)是匹配规则的唯一来源，PDF 与历史资料仅作参考。

模块职责见[实现说明](docs/订单簿恢复实现说明.md)，分析工具见 [analysis/](analysis/README.md)。
详细报告 `reports/` 和分析 notebook 仅本地保存，不随 Git 克隆提供。

开发检查和贡献流程见 [CONTRIBUTING](CONTRIBUTING.md)，变化记录见 [CHANGELOG](CHANGELOG.md)，
安全问题按 [SECURITY](SECURITY.md) 报告。

许可证为 [LGPL-3.0-only](LICENSE)：[LICENSE](LICENSE) 保存 LGPL v3 补充许可，
[COPYING](COPYING) 保存其引用的 GPL v3 全文，两者共同组成完整条款。来源声明见 [NOTICE](NOTICE)。
