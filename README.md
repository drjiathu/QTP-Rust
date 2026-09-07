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
  `O0→B0/T0/E0` 阶段帧。沪市 ETF 在 `20260706` 之前的收盘前驱为 `TRADE`，
  当天及以后与股票一样要求 `CCALL`；均按逐笔 `CLOSE` 检查点验收。
- `qtp-replay`：可执行程序，提供 `replay` 和 `validate` 子命令。

生产回放统一使用 `10_000` 作为价格乘数，即一个内部价格单位等于 `0.0001` 元；价格和
成交额均以整数计算，避免 `f64` 的精度和 `NaN` 问题。选择该乘数是为了无损承载深市
`Decimal(38,4)` 原始字段，并不表示股票的最小报价单位是 `0.0001` 元：股票普通限价和
成交价通常精确到分，ETF 通常精确到厘。

抽样数据中，所有出现非零第四位小数的深市委托均为 `OrdType='1'` 市价委托：其 `Price`
由参考价格按买入 `× 101%`、卖出 `× 99%` 计算，例如 `12.89 × 101% = 13.0189`、
`6.83 × 99% = 6.7617`。该值是市价单的价格边界/参考价格，不是普通限价或实际成交价；
市价委托的 `Price` 也可能为零。当前深市 CLI 默认使用 `RestAtLastTradePrice`：市价单
先隐藏，实际成交后按该成交价更新隐藏余量，仅在不再穿越对手盘时入簿；入簿后不再
重新定价。保护价不加入可见盘口。该实用策略可能产生即时成交／撤单之间的临时挂单，
快照对拍通过不代表逐事件路径被证明精确。本方最优有盘口时一次定价，无盘口时仍须
完成源撤单对账。`--sz-market-order-policy require-evidence` 保留严格待决模式，
`assume-contiguous` 保留诊断模式；默认实用策略的快照成功不算严格待决模式验收。
设计、限制及接口见[深市订单回放](docs/sz-order-replay.md)。原始非零
四位价格必须无损保留。因此生产代码固定使用四位价格尺度，成交额以 `u128` 累计并执行
checked arithmetic。官方 snapshot 中的
价格和成交额直接从 raw snapshot 的 `Decimal128` 转换为整数单位，不经过 `Float64`。
Snapshot 验证统一为开盘集合竞价结束、盘中交易、收盘集合竞价结束三个阶段，股票与 ETF
共用比较流程。选帧、时间窗、字段精度、收盘价和异常处理的唯一规范见
[Snapshot 验证匹配规则](docs/snapshot-validation-rules.md)。深市现已在完整单日回放后比较首条
E0；若成功应用了行情时间晚于 15:00 的事件，会报告并阻断未经阶段确认的收盘验收。
规范文档只定义验收契约；自动板块窗口、阶段链、重复静态帧校验及分类报告的实现状态见
[全市场回放与验证手册](docs/full-market-replay-guide.md)。

深市限价单统一按源价格、数量直接入簿，仅由实际成交和撤单改变余量，不因穿价而隐藏。
逐事件中间状态允许交叉，不代表交易所曾发布该中间盘口。恢复不再读取 raw snapshot
识别 `H0/V0` 复牌，也不要求跨 feed 复牌时间相等；snapshot 状态只用于独立验证。
市价单、本方最优及沪市策略不变，取消限价隐藏后的定价交互仍须跨日期回归。

无涨跌幅限制深市股票的 E0 验证使用独立的收盘有效价格范围比较视图；仅 validation
过滤 Rust 价位并重算范围内总量、加权价，不过滤参考 E0，不修改恢复账本和生产截面。
规则及缺失输入处理见[收盘比较规范](docs/snapshot-validation-rules.md#52-无涨跌幅限制股票的-e0-价格范围投影)。

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
[legacy 回放使用手册](docs/order-book-replay-guide.md)。当前有效的真实数据证据见
[20260828 验证基线](docs/real-data-validation.md)，被后续结论替代的实验过程保存在
[历史调查记录](docs/archive/real-data-validation-20260828-investigation.md)。

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
   深市离线输入的偶发逆序通过自然有序段归并修复临时分片，保留原始行号和审计，
   不依赖 `SecurityID` 或时间排序；见[处理说明](docs/full-market-replay-guide.md#深市偶发逆序处理)。
6. 使用 C++ golden、属性测试、合成 Parquet 和官方收盘帧分层验收。

## 参与贡献与许可证

贡献流程参见 [CONTRIBUTING.md](CONTRIBUTING.md)，版本变化记录在
[CHANGELOG.md](CHANGELOG.md)。安全问题请按 [SECURITY.md](SECURITY.md) 私下报告。

本项目采用 [GNU LGPL 3.0 only](LICENSE) 许可证。完整组合条款同时见
[COPYING](COPYING) 和 [COPYING.LESSER](COPYING.LESSER)，版权与来源说明见
[NOTICE](NOTICE)。
