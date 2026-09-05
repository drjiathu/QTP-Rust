# 订单簿回放使用手册

## 1. 环境与编译

Linux 上使用 `rustup` 安装 Rust，进入仓库后由 `rust-toolchain.toml` 自动选择 Rust 1.85.1：

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
cd /path/to/QTP-Rust
cargo build --locked
cargo build --release --locked
```

本章描述旧 C++ 数据结构的库 API。通联全市场 Parquet 的可执行程序参见
[沪深全市场 Parquet 回放与验证手册](full-market-replay-guide.md)。

原迁移开发方案已[归档](archive/cpp-to-rust-migration-plan.md)，其中保留旧字段／枚举映射、
有意行为差异和 oracle/golden 的历史依据，不再作为当前 API 草案或开发计划。归档不移除
本手册介绍的 legacy API，也不移除相关回归测试。

## 2. 输入约束

一次回放对应唯一的市场、交易日和证券。调用方分别提供 `OrderRecord` 和 `TradeRecord`
切片，两条流各自必须按正数 `sequence` 严格递增，并且不能跨流出现相同序号。

每条 raw 记录必须携带：

```rust
RawEventTime {
    steady_time: Option<LegacySteadyTimestampNs>,
    local_time: LocalTimestampNs,
    quote_time: QuoteTimestampNs,
}
```

`local_time` 和 `quote_time` 是 Unix epoch 纳秒且必须提供；可选的 legacy
`steady_time` 会在 normalization 时丢弃。回放只按 `sequence` 排序，不使用时间字段。

`PriceScale::from_decimal_places(4)` 表示 `10.1234` 转成内部整数价格 `101234`。raw 价格
必须有限、为正，并能在 `1e-6` 的缩放误差内表示为整数价格单位。

## 3. 最小回放示例

```rust
use std::{error::Error, io};
use qtp_core::*;

fn main() -> Result<(), Box<dyn Error>> {
    let trading_day = TradingDay::from_yyyymmdd(20_250_102)
        .ok_or_else(|| io::Error::other("invalid trading day"))?;
    let price_scale = PriceScale::from_decimal_places(4)
        .ok_or_else(|| io::Error::other("invalid price scale"))?;
    let book_key = BookKey {
        market: Market::Sse,
        trading_day,
        symbol: Symbol::from("600000.SH"),
    };
    let event_time = |sequence: i64| RawEventTime {
        steady_time: None,
        local_time: LocalTimestampNs::from_nanos(1_735_781_400_000_100_000 + sequence),
        quote_time: QuoteTimestampNs::from_nanos(1_735_781_400_000_000_000 + sequence),
    };
    let orders = vec![
        OrderRecord {
            event_time: event_time(1),
            symbol: book_key.symbol.clone(),
            kind: RawOrderType::LimitPrice,
            side: RawOrderSide::Buy,
            channel_no: 1,
            sequence: 1,
            order_id: 101,
            price: 10.00,
            quantity: 100,
        },
        OrderRecord {
            event_time: event_time(2),
            symbol: book_key.symbol.clone(),
            kind: RawOrderType::LimitPrice,
            side: RawOrderSide::Sell,
            channel_no: 1,
            sequence: 2,
            order_id: 202,
            price: 10.10,
            quantity: 80,
        },
    ];
    let trades = vec![TradeRecord {
        event_time: event_time(3),
        symbol: book_key.symbol.clone(),
        kind: RawTradeType::Trade,
        side: RawTradeSide::Unknown,
        channel_no: 1,
        sequence: 3,
        price: 10.05,
        quantity: 50,
        bid_order_id: 101,
        ask_order_id: 202,
    }];

    let context = LegacyContext {
        book_key: book_key.clone(),
        price_scale,
    };
    let mut book = OrderBook::new(BookConfig::new(book_key, price_scale));
    let mut replay = LegacyReplay::new(context);
    replay.replay(&mut book, &orders, &trades)?;

    println!("{:#?}", book.summary());
    println!("{:#?}", book.depth(5));
    Ok(())
}
```

把代码保存为 `examples/basic_replay.rs` 后执行：

```bash
cargo run --release --locked --example basic_replay
```

## 4. 查询结果

- `book.summary()`：最后成功序号和时间、最优买卖价、活动订单数及成交统计。
- `book.depth(n)`：买卖各前 `n` 档。
- `book.levels(Side::Buy)`：一侧全部价位；买方从高到低，卖方从低到高。
- `book.orders_at(side, price)`：指定价位的 FIFO 订单。
- `book.order(&order_key)`：查询指定活动订单；终态订单不会返回。

价格和成交额均为内部整数单位。展示价格应除以 `PriceScale::multiplier()`。

## 5. 未知成交引用策略

`BookConfig::new` 默认使用 `UnknownTradePolicy::Reject`：任何无法解析或已经不活动的成交引用
都会拒绝整条成交。对拍旧 C++ 时可使用：

```rust
let config = BookConfig::new(book_key, price_scale)
    .with_unknown_trade_policy(UnknownTradePolicy::UpdateKnownAndStatistics);
```

兼容模式会扣减已知订单并统计整笔成交，但 overfill 和数值溢出仍会拒绝事件。

## 6. 错误与恢复

`ReplayError` 提供错误类型、来源流、两个输入下标、待用 `apply_sequence` 和失败记录。
失败事件不会更新盘口、统计、元数据或历史引用索引。

修正数据后必须继续使用原来的 `LegacyReplay` 和 `OrderBook`，并从错误下标开始重放：

```rust
let result = replay.replay(&mut book, &orders, &trades);
if let Err(error) = result {
    replay.replay(
        &mut book,
        &corrected_orders[error.order_index..],
        &corrected_trades[error.trade_index..],
    )?;
}
```

不要跳过失败记录，也不要创建新的 `LegacyReplay`，否则会丢失已建立的历史订单引用关系。

## 7. 测试与性能检查

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cargo test --locked --doc
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo deny check
```

验证 C++ oracle 与 golden：

```bash
g++ -std=c++17 -O2 tests/cpp_oracle/order_book_oracle.cpp \
  -o /tmp/qtp-order-book-oracle
/tmp/qtp-order-book-oracle | \
  diff -u tests/fixtures/legacy_golden/order_book.txt -
```

性能冒烟测试：

```bash
cargo run --release --locked --example replay_benchmark -- 100000
```

该耗时包含内存中的 normalization、replay 和订单簿更新，不包含文件读取、通联解析和测试
数据构造。

## 8. legacy API 边界

legacy API 不读取通联文件，也不提供 Python、网络服务或持久化。生产 `qtp-replay` 使用
独立的通联 adapter，不复用 legacy raw 类型。两条链路的撤单都固定删除订单的全部未成交
余量，不支持部分撤单。
