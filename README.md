# QTP-Rust

QTP-Rust 是对 `QTP-core-main` 的 Rust 重构，支持旧 C++ 行为对拍，以及从通联/Datayes
单日全市场 Parquet 流式恢复沪深 A 股订单簿。

## 当前状态

- `market_data`：原始 `MarketDataRecord::{Order, Trade}`、核心强类型及
  `BookEvent::{AddOrder, OrderCancel, Trade}`。
- `legacy`：旧 QTP 映射规则、价格转换、历史订单引用解析和双流 replay。
- `order_book`：新增、全部余量撤单、部分/全部成交、隐藏穿越订单、FIFO、成交统计和只读查询。
- `production`：通联 Parquet Schema/来源校验、股票过滤、通道分片、沪深原生序号回放、
  固定间隔/收盘截面和官方 snapshot 三锚点验证。
- `qtp-replay`：可执行程序，提供 `replay` 和 `validate` 子命令。
- `docs/rust-migration-development-spec.md`：单 crate 的 C++ 订单簿迁移行为、接口、阶段和
  验收标准。

生产回放统一使用 `10_000` 作为价格乘数，即一个内部价格单位等于 `0.0001` 元；价格和
成交额均以整数计算，避免 `f64` 的精度和 `NaN` 问题。选择该乘数是为了无损承载深市
`Decimal(38,4)` 原始字段，并不表示股票的最小报价单位是 `0.0001` 元：股票普通限价和
成交价通常精确到分，ETF 通常精确到厘。

抽样数据中，所有出现非零第四位小数的深市委托均为 `OrdType='1'` 市价委托：其 `Price`
由参考价格按买入 `× 101%`、卖出 `× 99%` 计算，例如 `12.89 × 101% = 13.0189`、
`6.83 × 99% = 6.7617`。该值是市价单的价格边界/参考价格，不是普通限价或实际成交价；
回放仍按委托类型和对手盘确定订单语义，但原始四位价格必须无损保留。因此生产代码固定
使用四位价格尺度，成交额以 `u128` 累计并执行 checked arithmetic。官方 snapshot 中的
`Float64` 价格和成交额在比较前统一量化为 `round(value × 10_000)`，不直接比较浮点数，
也不以二进制浮点乘积必须接近整数作为有效性条件。

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
docs/                    设计和迁移记录
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
