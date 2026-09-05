# QTP-Rust

QTP-Rust 是对 `QTP-core-main` 的 Rust 重构，支持旧 C++ 行为对拍，以及从通联/Datayes
单日全市场 Parquet 流式恢复沪深 A 股订单簿。

## 当前状态

- `market_data`：原始 `MarketDataRecord::{Order, Trade}`、核心强类型及
  `BookEvent::{AddOrder, OrderCancel, Trade}`。
- `legacy`：旧 QTP 映射规则、价格转换、历史订单引用解析和双流 replay。
- `order_book`：新增、全部余量撤单、部分/全部成交、隐藏穿越订单、FIFO、成交统计和只读查询。
- `production`：通联 Parquet Schema/来源校验、股票过滤、通道分片、沪深原生序号回放、
  固定间隔/收盘截面，以及 raw snapshot 的开盘前、全部连续交易帧和收盘验证；沪市使用
  `MarketData` 的 `OCALL→TRADE/TRADE/CCALL→CLOSE`，深市使用 `mdl_6_28_0` 的
  `O0→B0/T0/E0` 阶段帧。
- `qtp-replay`：可执行程序，提供 `replay` 和 `validate` 子命令。

生产回放统一使用 `10_000` 作为价格乘数，即一个内部价格单位等于 `0.0001` 元；价格和
成交额均以整数计算，避免 `f64` 的精度和 `NaN` 问题。选择该乘数是为了无损承载深市
`Decimal(38,4)` 原始字段，并不表示股票的最小报价单位是 `0.0001` 元：股票普通限价和
成交价通常精确到分，ETF 通常精确到厘。

抽样数据中，所有出现非零第四位小数的深市委托均为 `OrdType='1'` 市价委托：其 `Price`
由参考价格按买入 `× 101%`、卖出 `× 99%` 计算，例如 `12.89 × 101% = 13.0189`、
`6.83 × 99% = 6.7617`。该值是市价单的价格边界/参考价格，不是普通限价或实际成交价；
市价委托的 `Price` 也可能为零：有对手盘时可以由对手方最优价执行，无对手盘时不存在
可解析的有效价格。市价单提交时不把原始保护价加入可见盘口；发生部分成交后，如果仍有
余量且已经不再穿越对手盘，则以最后成交价转为可见剩余委托。完全未成交且没有成交价的
市价单继续以无价隐藏订单保留引用，等待后续成交或撤单。深市本方最优单在本方盘口为空时
仍采用无价隐藏表达并等待后续撤单；不得虚构价格或将保护价当作剩余委托价格。原始非零
四位价格必须无损保留。因此生产代码固定使用四位价格尺度，成交额以 `u128` 累计并执行
checked arithmetic。官方 snapshot 中的
价格和成交额直接从 raw snapshot 的 `Decimal128` 转换为整数单位，不经过 `Float64`。
Snapshot 验证统一为开盘集合竞价结束、盘中交易、收盘集合竞价结束三个阶段，股票与 ETF
共用比较流程。选帧、时间窗、字段精度、收盘价和异常处理的唯一规范见
[Snapshot 验证匹配规则](docs/snapshot-validation-rules.md)。该文档区分目标规则与当前实现，
包括尚待落实的深市“完整单日回放后比较首条 E0”，不能把目标规则视为当前运行行为。

深市盘中复牌通过原始 `mdl_6_28_0` 的 `H0 → 首个 T0` 状态识别。复牌检查点同毫秒
批量发布的限价委托按集合竞价入簿，后续毫秒恢复连续竞价规则，避免未成交竞价委托被
`HideIfCrossing` 持续隐藏。`replay` 和 `validate` 使用同一规则；回放仅投影状态、时间、
证券代码和来源行号，不使用参考盘口初始化订单簿。仅有逐笔而缺少状态文件时，报告
`sz_phase_source_available=false`，无法据此识别盘中复牌。

## 开发环境

- Rust 1.85.1（Edition 2024）
- `rustfmt`
- `clippy`

安装 Rust 后，仓库中的 `rust-toolchain.toml` 会让 `rustup` 自动选择所需版本。

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cargo deny check
```

生产 Parquet 命令参见
[全市场回放与验证手册](docs/full-market-replay-guide.md)；旧 C++ 数据 API 参见
[legacy 回放使用手册](docs/order-book-replay-guide.md)。真实数据冒烟结果和集合竞价帧的数据
质量结论见 [真实数据验证记录](docs/real-data-validation.md)。

原 C++ 迁移方案已移入
[历史归档](docs/archive/cpp-to-rust-migration-plan.md)，仅用于追溯旧字段映射、兼容行为和
迁移决策，不再作为当前开发或验收规范。归档不影响 legacy 实现及 C++ golden 测试。

## 目录

```text
src/
  lib.rs                 crate 入口和公共导出
  market_data/           raw 记录和核心事件
  legacy/                normalization、引用解析和 replay
  order_book/            订单簿状态机、错误和只读 view
  production/            Parquet 输入、通道分片、截面和验证
  bin/qtp-replay.rs      生产命令入口
tests/                   集成、属性测试、C++ oracle 和 golden
examples/                replay 外围性能测量
docs/                    使用手册、验证规范和实验记录
docs/archive/            不再维护的历史设计与迁移方案
```

## v1 行为

1. 原始记录显式转换为 `AddOrder`、`OrderCancel`、`Trade`。
2. 价格档、订单 FIFO、新增、成交和撤销全部未成交余量保持失败原子性。
3. 委托/成交双流只按原始序号归并，同序号视为歧义。
4. 默认拒绝未知成交引用；C++ 对拍可选择只更新已知订单并统计成交。
5. 生产回放按通道原生序号处理单日全市场数据，普通截面严格包含 `quote_time < T` 的事件。
6. 使用 C++ golden、属性测试、合成 Parquet 和官方收盘帧分层验收。

## 参与贡献与许可证

贡献流程参见 [CONTRIBUTING.md](CONTRIBUTING.md)，版本变化记录在
[CHANGELOG.md](CHANGELOG.md)。安全问题请按 [SECURITY.md](SECURITY.md) 私下报告。

本项目采用 [GNU LGPL 3.0 only](LICENSE) 许可证。完整组合条款同时见
[COPYING](COPYING) 和 [COPYING.LESSER](COPYING.LESSER)，版权与来源说明见
[NOTICE](NOTICE)。
