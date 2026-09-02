# QTP-Rust

QTP-Rust 是对 `QTP-core-main` 的 Rust 重构。当前第一目标是根据逐笔委托和逐笔成交恢复
沪深 A 股订单簿，并通过与旧 C++ 实现逐事件对拍保证兼容性。

## 当前状态

- `market_data`：原始 `MarketDataRecord::{Order, Trade}`、核心强类型及
  `BookEvent::{AddOrder, OrderCancel, Trade}`。
- `legacy`：旧 QTP 映射规则、价格转换、历史订单引用解析和双流 replay。
- `order_book`：新增、全部余量撤单、部分/全部成交、隐藏穿越订单、FIFO、成交统计和只读查询。
- `docs/rust-migration-development-spec.md`：单 crate 的 C++ 订单簿迁移行为、接口、阶段和
  验收标准。

价格按每个订单簿固定的 scale 存储为整数价格单位，避免以 `f64` 作为有序容器键时出现
精度和 `NaN` 问题。整数价格单位不等同于交易所最小变动价位。

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

订单簿回放的完整接入示例、错误恢复和结果查询参见
[订单簿回放使用手册](docs/order-book-replay-guide.md)。

## 目录

```text
src/
  lib.rs                 crate 入口和公共导出
  market_data/           raw 记录和核心事件
  legacy/                normalization、引用解析和 replay
  order_book/            订单簿状态机、错误和只读 view
tests/                   集成、属性测试、C++ oracle 和 golden
examples/                replay 外围性能测量
docs/                    设计和迁移记录
```

## v1 行为

1. 原始记录显式转换为 `AddOrder`、`OrderCancel`、`Trade`。
2. 价格档、订单 FIFO、新增、成交和撤销全部未成交余量保持失败原子性。
3. 委托/成交双流只按原始序号归并，同序号视为歧义。
4. 默认拒绝未知成交引用；C++ 对拍可选择只更新已知订单并统计成交。
5. 使用轻量 C++ oracle、固定 golden 和属性测试逐事件验收。

通联逐笔文档和 Clara 项目在本阶段只作为数据语义参考；供应商解析、数据清洗、Python/FFI
和实时服务不属于首个里程碑。

## 参与贡献与许可证

贡献流程参见 [CONTRIBUTING.md](CONTRIBUTING.md)，版本变化记录在
[CHANGELOG.md](CHANGELOG.md)。安全问题请按 [SECURITY.md](SECURITY.md) 私下报告。

本项目采用 [GNU LGPL 3.0 only](LICENSE) 许可证。完整组合条款同时见
[COPYING](COPYING) 和 [COPYING.LESSER](COPYING.LESSER)，版权与来源说明见
[NOTICE](NOTICE)。
