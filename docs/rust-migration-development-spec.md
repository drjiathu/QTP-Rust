# QTP 订单簿 C++ → Rust 迁移开发方案

> 状态：v1 开发基线
> 目标对象：`QTP::Quote::Generato::OrderBook`
> 目标 crate：`qtp-core`
> 适用市场：沪深 A 股逐笔委托、逐笔成交回放

## 1. 结论

第一阶段只迁移原 C++ 项目的订单簿恢复功能，不同时建设通联文件解析、Clara 数据清洗、
Python 接口、实时服务或通用交易所框架。

Rust v1 采用单 crate、分模块实现：

```text
原始逐笔记录
    MarketDataRecord::{Order, Trade}
                │
                ▼
legacy replay（归并并分配 apply sequence）
                │
                ▼
legacy normalization
                │
                ▼
标准订单簿事件
    BookEvent::{AddOrder, OrderCancel, Trade}
                │
                ▼
OrderBook 状态机
    订单索引 + 价格档 FIFO + 成交统计
```

迁移原则是：

1. 迁移 C++ 在合法输入下已经具备的有效行为。
2. 用 Rust 的类型、检查运算和失败原子性修复旧实现缺陷。
3. 旧 C++ 的特殊行为放入明确的 `LegacyQtpRules` 或兼容策略，不污染默认严格语义。
4. 通联文档和 Clara 项目只用于理解字段含义及数据源特性，不作为 v1 依赖，也不直接决定
   核心模型。

## 2. v1 范围

### 2.1 本期实现

- 逐笔委托与逐笔成交的原始类别模型。
- 旧 QTP `Entrust`、`Transaction` 到 Rust 事件的字段对应和标准化。
- 委托/成交双流的确定性顺序归并。
- 新增委托、部分成交、全部成交和撤销全部未成交余量。
- 买卖价格档、档内 FIFO、聚合剩余量和活动订单索引。
- 穿越对手最优价订单的隐藏状态及剩余量重新入簿。
- 最新成交价、最高价、最低价、累计成交量、累计成交额和成交笔数。
- 深度、价位、档内订单、活动订单和汇总查询。
- Rust 单元测试、属性测试、C++ 轻量 oracle 和固定 golden 对拍。

### 2.2 本期不实现

- 通联原始文件、DataFrame 或其他供应商协议解析。
- Clara 的清洗、拆分或修复算法。
- Python/PyO3、C ABI、C++ FFI。
- 实时行情服务、异步运行时、线程分片和背压。
- 持久化、快照恢复、网络 API 和监控服务。
- 多 crate workspace。
- 跨市场统一规则引擎或未由旧 C++ 覆盖的新订单类型。
- 主动撮合；订单簿只应用输入中的成交事实。

跨证券或跨交易日不复用同一个 `OrderBook`。v1 不增加 `Session`、`Reset` 事件，而是创建
新的订单簿实例。

## 3. 行为基线与有意修复

### 3.1 需要兼容的 C++ 有效行为

| C++ 行为 | Rust v1 |
| --- | --- |
| 按买卖方向维护价格树 | 两个 `BTreeMap<Price, PriceLevel>` |
| 同价位委托按到达顺序排列 | 价位内双向 FIFO 链 |
| 成交减少订单和档位剩余量 | 检查后原子更新 |
| 部分成交保留原 FIFO 位置 | 保持不变 |
| 全部成交删除订单和空价位 | 保持不变 |
| 撤单删除当前全部剩余量 | `OrderCancel` |
| 连续竞价兼容窗口内隐藏穿越订单 | `OrderLocation::Aggressive` |
| 隐藏订单成交后满足条件时将剩余量重新入簿 | 追加到对应价位队尾 |
| 更新成交统计 | 每个成功 `Trade` 只统计一次 |

“隐藏穿越订单”不是某一种订单类型的专属行为。旧代码在兼容时间窗口内，只要最终委托价
穿越对手最优价，就先将订单保存在活动索引中而不放入可见价位。

隐藏订单收到成交后仍有余量时：

- 买单在对手簿为空，或委托价严格低于当前最优卖价时，重新进入买方价位。
- 卖单在对手簿为空，或委托价严格高于当前最优买价时，重新进入卖方价位。
- 与对手最优价相等时仍保持隐藏。
- 重新入簿的剩余订单追加在该价位 FIFO 队尾。

### 3.2 不复制的旧实现缺陷

| 旧实现问题 | Rust 处理 |
| --- | --- |
| `double` 作为价格树键 | 按每簿 scale 转换为整数价格单位 |
| 空盘口直接读取最优价 | 返回结构化错误，状态不变 |
| 活动期重复会破坏价位，终态后又允许同号复用 | 保留轻量 tombstone，簿生命周期内拒绝任何重复 `OrderKey` |
| 成交过量导致无符号下溢 | `checked_*` 校验，拒绝整条事件 |
| 一侧已更新后另一侧失败 | validate / plan / commit，保证失败原子性 |
| 撤单分支把任何非 BUY 方向当成 SELL | 只接受 `Buy`/`Sell`，其他方向拒绝 |
| 最低价以 `DBL_MAX` 表示无值 | `Option<Price>` |
| 入队时提前更新结果时间 | 仅在事件成功应用后推进应用元数据 |
| 订单只按 `order_no` 索引 | `BookKey` 上下文内使用复合 `OrderKey` |
| 不验证双流顺序和同序号歧义 | replay 层显式检查 |

旧 C++ 的 `open_interest` 始终为零，不进入订单簿核心统计。若以后需要旧输出结构的兼容视图，
由视图层填零。

## 4. 分层领域模型

### 4.1 标量与身份

内部价格不得使用 `f64`：

```rust
pub struct Price(i64);          // 按 BookConfig.price_scale 存储的价格单位
pub struct Quantity(u64);
pub struct OrderId(u64);
pub struct RawSequence(u64);
pub struct ApplySequence(u64);
pub struct ChannelId(u32);
pub struct LocalTimestampNs(i64);  // 系统时钟域：本地接收/生成时间
pub struct QuoteTimestampNs(i64);  // 系统时钟域：行情时间

pub struct BookKey {
    pub market: Market,         // SSE / SZSE
    pub trading_day: TradingDay,
    pub symbol: Symbol,
}

pub struct OrderKey {
    pub channel_id: ChannelId,
    pub side: Side,
    pub order_id: OrderId,
}
```

一个 `OrderBook` 在构造时固定一个 `BookKey`，因此 `OrderKey` 只需在该实例的完整生命周期
内唯一。`Side` 只能是 `Buy` 或 `Sell`。

旧 C++ 没有可靠提供的市场和交易日由回放上下文显式补充，不能从订单号猜测。

成交记录中的买卖委托引用必须分别构造。不能未经数据源规则确认，就把成交记录自身的
`channel_id` 同时套用到买卖两侧引用；legacy adapter 必须把来源规则和校验写清楚。

### 4.2 价格转换

`BookConfig` 固定该订单簿的 `price_scale`：

```text
display_price = price_units × price_scale
```

旧结构中的 `f64` 价格进入核心前必须满足：

1. 有限，不是 `NaN` 或无穷。
2. 对需要输入价格的委托和成交，价格大于零。
3. `raw_price / price_scale` 在约定浮点容差内接近整数。
4. 转换结果位于 `i64` 范围。

容差只吸收十进制价格经过二进制浮点表示产生的微小误差，不能做价格取整或猜测。
交易所最小变动价位与内部存储精度不是同一个概念，因此统一使用“整数价格单位”，不称为 tick。

### 4.3 原始逐笔记录

必须保留 A 股行情的两个原始类别：

```rust
pub enum MarketDataRecord {
    Order(OrderRecord),
    Trade(TradeRecord),
}
```

对应关系为：

| 原 C++ | Rust 原始层 | 含义 |
| --- | --- | --- |
| `Entrust` | `OrderRecord` | 一条逐笔委托记录 |
| `Transaction` | `TradeRecord` | 一条逐笔成交记录 |

`OrderRecord` 不等于新增订单，`TradeRecord` 也不保证一定是成交。旧项目中两种记录都可能
编码撤单，必须先看记录类型，再标准化成订单簿事件。

原始层可以保留旧字段中的零值哨兵、`f64` 价格和旧枚举，但这些值不得直接进入核心。
时间字段使用独立类型：

```rust
pub struct LegacySteadyTimestampNs(i64);
pub struct LocalTimestampNs(i64);
pub struct QuoteTimestampNs(i64);

pub struct RawEventTime {
    pub steady_time: Option<LegacySteadyTimestampNs>,
    pub local_time: LocalTimestampNs,
    pub quote_time: QuoteTimestampNs,
}
```

`OrderRecord` 和 `TradeRecord` 都必须携带 `RawEventTime`。`steady_time` 只是旧 QTP 输入可能
提供的兼容字段；其时钟起点只在原进程内有意义，因此 normalization 必须丢弃它。
`local_time` 和 `quote_time` 是当前输入契约的必需字段，不使用 `Option`。

公共字段对应：

| 来源字段 | Rust 去向 | 规则 |
| --- | --- | --- |
| `symbol` | `EventMeta.book_key.symbol` | 必须与回放上下文一致 |
| 无可靠对应字段 | `BookKey.market`、`BookKey.trading_day` | 由回放上下文提供 |
| `channel_no` | 原始事件通道 | 新增委托可用于构造 `OrderKey`；成交引用需经 resolver |
| `seq` | `EventMeta.raw_sequence` | 必须为正并用于双流排序 |
| C++ `steady_time` | `RawEventTime.steady_time` | 可选 legacy 字段；normalization 时丢弃 |
| C++ `local_time`、通联 `LocalTime` | `RawEventTime.local_time` → `EventMeta.local_time` | 必需的本地接收/生成时间 |
| C++ `quote_time`、通联 `TickTime`/`TransactTime` | `RawEventTime.quote_time` → `EventMeta.quote_time` | 必需的行情事件时间 |

当前范围内，旧 C++ 的 `Entrust`、`Transaction` 都携带 `local_time`。通联 V4.1 中本项目涉及
的三类股票逐笔文件也都提供 `LocalTime`：

| 通联文件 | 行情事件时间 | 本地接收时间 |
| --- | --- | --- |
| 上交所合并逐笔 `mdl_4_24_0` | `TickTime` | `LocalTime` |
| 深交所逐笔委托 `mdl_6_33_0` | `TransactTime` | `LocalTime` |
| 深交所逐笔成交 `mdl_6_36_0` | `TransactTime` | `LocalTime` |

通联字段只有“时:分:秒.毫秒”时，adapter 必须使用 `BookKey.trading_day` 和
`Asia/Shanghai` 时区组成完整的 `LocalTimestampNs`、`QuoteTimestampNs`。缺失、空值或
非法 `LocalTime` 必须在创建 `MarketDataRecord` 前报错；禁止用当前时间、零值、
`quote_time` 或其他字段补齐。

三种原始时间均保留纳秒精度，但不同 newtype 之间禁止直接比较或相减。核心只接收
`LocalTimestampNs` 和 `QuoteTimestampNs`，不定义 `SteadyTimestampNs`。

`Entrust` → `OrderRecord`：

| C++ 字段 | Rust 原始字段/用途 |
| --- | --- |
| `type` | 原始委托记录类型；决定新增或撤单及定价指令 |
| `side` | 新增/撤单目标方向，标准化为 `Side` |
| `order_no` | `OrderKey.order_id` |
| `price` | `Provided` 价格；其他类型按 legacy 规则处理 |
| `volume` | 新增数量；撤单记录中忽略 |

`Transaction` → `TradeRecord`：

| C++ 字段 | Rust 原始字段/用途 |
| --- | --- |
| `type` | 决定普通成交或撤单 |
| `bs_flag` | 保留用于审计；旧订单簿核心未使用 |
| `bid_no` | 买方订单引用或撤单目标候选 |
| `ask_no` | 卖方订单引用或撤单目标候选 |
| `price` | 普通成交价格；撤单记录中忽略 |
| `volume` | 普通成交数量；撤单记录中忽略 |

legacy replay 维护成功新增订单的历史索引
`(Side, OrderId) → Unique(OrderKey) | Ambiguous`，分别解析 `bid_no` 和 `ask_no`：

- 只有 `AddOrder` 成功应用后才登记，终态后不删除。
- 唯一命中生成 `Resolved`，未命中生成 `Unresolved`。
- 同方向、同订单号在多个通道命中时返回 `AmbiguousOrderReference`。
- 成交流撤单必须唯一命中；普通成交允许未命中。

不得默认复用 `Transaction.channel_no`，也不能把旧 Insight adapter 疑似反置的赋值当作
事实。

### 4.4 核心事件

v1 的核心事件只有三种：

```rust
pub enum BookEvent {
    AddOrder(AddOrder),
    OrderCancel(OrderCancel),
    Trade(Trade),
}

pub struct EventMeta {
    pub book_key: BookKey,
    pub raw_sequence: RawSequence,
    pub apply_sequence: ApplySequence,
    pub local_time: LocalTimestampNs,
    pub quote_time: QuoteTimestampNs,
}

pub struct AddOrder {
    pub meta: EventMeta,
    pub order_key: OrderKey,
    pub pricing: PricingInstruction,
    pub crossing: CrossingBehavior,
    pub quantity: Quantity,
}

pub enum PricingInstruction {
    Provided(Price),
    SameSideBest,
    OppositeBest,
    Unpriced,
}

pub enum CrossingBehavior {
    Rest,
    HideIfCrossing,
    RestAtLastTradePrice,
    AlwaysHide,
}

pub struct OrderCancel {
    pub meta: EventMeta,
    pub order_key: OrderKey,
}

pub struct Trade {
    pub meta: EventMeta,
    pub bid_order: OrderReference,
    pub ask_order: OrderReference,
    pub price: Price,
    pub quantity: Quantity,
}

pub enum OrderReference {
    Absent,
    Resolved(OrderKey),
    Unresolved { side: Side, order_id: OrderId },
}
```

`Absent` 表示源记录明确没有该侧订单引用；`Resolved` 表示历史索引能够恢复完整
`OrderKey`；`Unresolved` 表示来源提供了订单号，但此前没有可唯一匹配的新增委托。这三种
情况不能混为一谈。

本方最优和对手最优的定价依赖当前盘口。因此 normalization 只完成合法性检查，并由
`LegacyQtpRules` 把旧订单类型映射成 `PricingInstruction`，同时按 `quote_time` 生成
`CrossingBehavior`；实际有效价格和是否穿越由 `OrderBook::apply` 在写入前解析。订单簿
核心不直接解释旧枚举或交易时段。

`Unpriced + AlwaysHide` 是后续生产通联适配器使用的标准语义，不改变 legacy 映射：它表示
来源确实没有可解析价格、但订单号仍需供下一笔成交或撤单引用的临时订单。该订单从不进入
可见价位，也不允许成交后重入。`Unpriced` 与 `Rest` 或 `HideIfCrossing` 组合必须返回
`BookError::UnpricedVisibleOrder`，禁止用零值或任意哨兵价补齐。

深市 `OrdType='1'` 市价单使用 `RestAtLastTradePrice`：新增时作为主动订单隐藏，原始
`Price` 只保留保护价/边界价语义；每次成交更新有效价格。如果成交后仍有余量且不再穿越
对手盘，余量以最后成交价进入可见盘口。完全未成交、没有成交价的订单仍保持隐藏。该行为
与无条件 `AlwaysHide` 不同。

### 4.5 撤单语义

沪深 A 股 v1 不建模“只减少当前剩余量的一部分”。`OrderCancel` 的唯一语义是：

> 撤销目标委托在该时刻的全部未成交余量。

因此：

- 不定义 `CancelQuantity`。
- `OrderCancel` 不携带数量。
- 已部分成交的订单收到撤单时，一次性删除其全部 `remaining_quantity`。
- 撤销 `Resting` 订单时扣除档位聚合量并从 FIFO 解链；撤销 `Aggressive` 订单时不修改可见
  价位。
- 两种位置的订单都从活动索引和订单存储中删除。
- 原始消息若带有撤单数量，只能供 adapter 校验或审计，不能控制核心扣减量。
- `ApplyOutcome::Cancelled` 返回实际撤销的剩余数量，便于验证和记录。

## 5. 旧结构到核心事件的对应

| C++ 记录 | 旧类型 | Rust 结果 |
| --- | --- | --- |
| `Entrust` | `LIMIT_PRICE` | `AddOrder { pricing: Provided(input_price) }` |
| `Entrust` | `MARKET_PRICE` | `AddOrder { pricing: OppositeBest }` |
| `Entrust` | `FORWARD_BEST_PRICE` | `AddOrder { pricing: SameSideBest }` |
| `Entrust` | `REVERSE_BEST_PRICE` | `AddOrder { pricing: Provided(input_price) }` |
| `Entrust` | `CANCELED` | `OrderCancel` |
| `Transaction` | `TRADE` | `Trade` |
| `Transaction` | `CANCELED` | `OrderCancel` |

先由 replay 为归并后的原始记录分配应用序号，再执行可失败转换：

```rust
fn normalize(
    record: SequencedMarketDataRecord,
    context: &LegacyContext,
) -> Result<BookEvent, NormalizeError>;
```

不能使用无条件 `From`。校验必须按记录类型执行：新增委托和普通成交需要合法价格与正数量；
旧 C++ 撤单记录的 `price`、`volume` 不参与核心语义，最多用于 adapter 审计。以下输入必须在
normalization 阶段报错：

- 该记录类型所需的方向、订单类型、订单号、通道号、价格或数量非法。
- 记录的证券与回放 `BookKey` 不一致。
- 撤单目标缺失或买卖目标同时存在而含义不明确。
- 无法确定买卖订单引用各自所属通道。
- 浮点价格不能精确映射到配置的整数价格单位。

旧 `Transaction` 撤单代码采用“`bid_no != 0` 则撤买单，否则撤 `ask_no`”的优先规则。
Rust 不把两个编号都非零或都为零的歧义带入核心；legacy normalization 应显式校验并记录
与旧适配器的差异。

旧 Insight adapter 中还存在买卖委托编号疑似反置和结构字段不一致。字段迁移必须以
“显式对应 + fixture 验证”完成，不能复制旧 adapter 的赋值代码。

## 6. `LegacyQtpRules`

v1 只实现旧 C++ 已经使用的定价和时间行为：

| 旧类型 | 有效价格规则 |
| --- | --- |
| `LIMIT_PRICE` | `Provided(input_price)` |
| `MARKET_PRICE` | `OppositeBest` |
| `FORWARD_BEST_PRICE` | `SameSideBest` |
| `REVERSE_BEST_PRICE` | `Provided(input_price)`，即旧代码实际使用输入价格 |

`REVERSE_BEST_PRICE` 的行为是历史兼容结论，不是根据名称推导出的交易所规则；实现中必须
单独分支并写注释，不能依赖默认 fallthrough。

读取 `SameSideBest` 或 `OppositeBest` 时，如果所需价位不存在，返回
`BookError::ReferencePriceUnavailable`，且不得先创建订单或更新任何元数据。

旧 C++ 判断连续竞价的北京时间窗口为：

```text
[09:30:00, 14:57:00)
```

该规则只读取 `EventMeta.quote_time`。`local_time` 仅用于来源审计、延迟分析和最后成功事件
元数据，不参与排序、时间窗口判断或订单簿状态计算。

该判断包含午间休市，只是兼容旧实现的历史行为，不代表完整交易所时段定义。窗口外的穿越
订单按旧实现直接进入可见价位。v1 将此行为封装在 `LegacyQtpRules`，不做通用市场日历。

## 7. OrderBook 内部设计

### 7.1 状态结构

```text
OrderBook
├── config: BookConfig
├── bids: BTreeMap<Price, PriceLevel>
├── asks: BTreeMap<Price, PriceLevel>
├── active_by_key: HashMap<OrderKey, OrderHandle>
├── seen_order_keys: HashSet<OrderKey>
├── orders: SlotMap<OrderHandle, OrderState>
├── statistics: TradeStatistics
└── last_applied_meta: Option<EventMeta>
```

`PriceLevel` 保存：

- `total_quantity`
- `order_count`
- FIFO 的 `head` / `tail`

`OrderState` 保存：

- `order_key`
- `effective_price`
- `original_quantity`
- `remaining_quantity`
- `previous` / `next`
- `OrderLocation::{Resting, Aggressive}`

活动订单通过 `SlotMap` 获得稳定 handle；对外 API 不暴露 handle。价格档和订单之间不保存
裸指针或 Rust 引用。

该结构支持：

- 查找价格档：`O(log P)`。
- 查找订单：均摊 `O(1)`。
- 任意活动订单撤单或全成时从 FIFO 解链：`O(1)`。
- 同价位尾部追加：`O(1)`。

filled/cancelled 订单从活动存储中删除，但其 key 保留在 `seen_order_keys` 作为轻量 tombstone。
同一个 `BookKey` 生命周期内，已完成或已撤销的 `OrderKey` 也不得重新新增。v1 的 `order()`
只查询活动订单；终态数量通过 `ApplyOutcome` 和回放日志审计，不在核心保存完整历史状态。

`last_applied_meta` 只在事件成功应用后更新，`summary()` 可以返回其中的 `local_time` 和
`quote_time`，但不得返回 legacy `steady_time`。旧 C++
`Result` 在带参 `Update` 收到记录时就覆盖时间，即使记录仍在队列中；Rust 将其修正为“最后
成功应用事件”的时间语义。若以后需要完全复刻旧到达时间，必须放在独立兼容视图中。

### 7.2 核心不变量

每次成功应用事件后必须满足：

1. 活动订单满足 `0 < remaining_quantity <= original_quantity`；`traded_quantity` 由两者
   之差计算，不保存第三份可失配状态。
2. 每个 `OrderKey` 最多对应一个活动订单，且所有活动 key 都存在于 `seen_order_keys`。
3. `seen_order_keys` 中的 key 在该 `BookKey` 生命周期内不能再次新增。
4. `Resting` 订单恰好属于一个方向、一个价格档和一条 FIFO 链。
5. `Aggressive` 订单存在于活动索引，但不属于任何可见价位。
6. 价位聚合量等于该价位所有订单余量之和。
7. 价位订单数、`head`、`tail` 与双向链完全一致。
8. 不保留数量为零的订单或空价格档。
9. 买方最佳价为最高买价，卖方最佳价为最低卖价。
10. 数量、金额、序号和统计运算均不得溢出或下溢。

debug/test 构建提供 `check_invariants()`；属性测试在每个事件后调用。

### 7.3 失败原子性

`apply()` 按以下模式实现：

```text
validate → build mutation plan → commit
```

commit 前完成：

- `BookKey` 和应用序号检查；首个序号必须为 1，之后必须等于
  `last_applied_meta.apply_sequence.checked_add(1)`。
- 订单引用、方向和位置检查。
- 全生命周期重复订单、未知撤单和未知成交策略检查。
- 两侧成交余量检查。
- 档位、订单、成交量和成交额的全部 checked arithmetic。
- 状态依赖型委托价格解析。

发生任何错误时，盘口、订单索引、统计、最后序号以及最后成功事件的 `local_time`、
`quote_time` 必须逐字段保持不变。

### 7.4 成交引用策略

默认核心使用严格策略：

```rust
pub enum UnknownTradePolicy {
    Reject,
    UpdateKnownAndStatistics,
}
```

- `Reject`：任一 `Some(order_key)` 未知时拒绝整条成交，状态不变。
- `UpdateKnownAndStatistics`：兼容旧 C++；在不存在 overfill、overflow 等其他错误时，即使有
  未知引用也计入一次成交统计，并扣减已知侧。
- 任一已知订单发生 overfill 时，两种策略都拒绝整条事件。
- 数据源明确给出的 `None` 不算未知引用。

旧 C++ 对拍必须使用 `UpdateKnownAndStatistics`；新接入默认使用 `Reject`，是否切换由上游数据
契约决定。这一兼容开关只处理未知成交引用，不扩展为部分撤单或其他宽松行为。

### 7.5 成交统计

只有成功应用的 `Trade` 更新：

- `last_price: Option<Price>`
- `high_price: Option<Price>`
- `low_price: Option<Price>`
- `total_quantity`
- `total_turnover_units`
- `trade_count`

一条成交无论引用零个、一个还是两个订单，都只统计一次。成交额用能够覆盖配置上限的整数
类型并执行 checked arithmetic；对外展示时再结合 `price_scale` 转换。

## 8. 双流 replay

核心 `OrderBook` 只接收已经确定全序的 `BookEvent`。旧 C++ 的委托队列、成交队列合并逻辑
迁移到 `legacy::replay`，对两个 `MarketDataRecord` 流先归并并分配应用序号，再交给
normalization 生成 `BookEvent`，不把队列放进订单簿状态机。

replay 规则：

```rust
pub struct SequencedMarketDataRecord {
    pub apply_sequence: ApplySequence,
    pub record: MarketDataRecord,
}
```

1. 委托流和成交流各自必须按 `raw_sequence` 严格递增。
2. 两种序号必须来自同一个可比较的序号域；由 adapter 声明该前提。
3. 使用标准双路归并，较小序号先输出。
4. 一侧结束后完整排空另一侧尾部。
5. 跨流出现相同 `raw_sequence` 时，严格模式返回 `AmbiguousSequence`。
6. 为归并结果分配从一开始连续递增的 `apply_sequence`。
7. `raw_sequence` 只用于来源排序；除非来源契约明确声明稠密，否则不把跳号当作数据缺口。
8. 不得使用 `local_time`、`quote_time` 或 legacy `steady_time` 解决同序号歧义。

旧 C++ 同序号结果依赖两类记录的到达顺序；Rust 不保留这种不确定性。旧实现正常无异常路径
下最终 flush 通常只有一侧尾部待处理，因此不得把 flush 描述成固定“委托优先”规则。

每个成功事件才更新 `last_applied_meta`。失败事件不能被标记为已经应用。
normalization 或 `apply` 任一步失败时，replay 必须停止，并在修正后重试同一原始记录和同一
应用序号；不得跳过失败记录后继续提交 `n + 1`。

## 9. 公共 API 草案

v1 先提供具体类型，不提前抽象 trait：

```rust
impl OrderBook {
    pub fn new(config: BookConfig) -> Self;

    pub fn apply(
        &mut self,
        event: BookEvent,
    ) -> Result<ApplyOutcome, BookError>;

    pub fn summary(&self) -> BookSummary;
    pub fn depth(&self, levels: usize) -> DepthView;
    pub fn levels(&self, side: Side) -> Vec<LevelView>;
    pub fn orders_at(
        &self,
        side: Side,
        price: Price,
    ) -> Vec<OrderView>;
    pub fn order(&self, key: &OrderKey) -> Option<OrderView>;
}
```

查询接口只返回只读 view，不允许调用方修改内部数量、链表或索引。

`ApplyOutcome` 至少区分：

- 新增订单及其可见/隐藏位置。
- 成交后买卖两侧的实际扣减和终态。
- 撤单实际删除的全部剩余数量。

## 10. 单 crate 模块布局

```text
src/
├── lib.rs
├── market_data/
│   ├── mod.rs
│   └── types.rs
├── legacy/
│   ├── mod.rs
│   ├── normalization.rs
│   ├── references.rs
│   ├── replay.rs
│   └── rules.rs
└── order_book/
    ├── mod.rs
    ├── book.rs
    ├── error.rs
    ├── state.rs
    └── view.rs
```

仍然只有一个 `qtp-core` crate。

生产依赖：

- `slotmap`：稳定订单 handle。
- `thiserror`：结构化错误。

开发依赖：

- `proptest`：随机事件序列和不变量测试。

v1 不为尚未实现的接口引入 Tokio、PyO3、serde 或供应商 SDK。

## 11. 测试与 C++ 对拍

### 11.1 Rust 测试矩阵

标准化：

- `Entrust`/`Transaction` 的每种合法映射。
- 旧 QTP raw `steady_time` 的 `Some`/`None` 都可接收，且改变其值不影响 `BookEvent`。
- `local_time`、`quote_time` 使用不同的必需类型并正确进入 `EventMeta`。
- 非法方向、零/负标识、非法价格、零数量和 `BookKey` 不匹配。
- 撤单目标缺失、双目标歧义和买卖编号对应。
- `f64` 到整数价格单位的精确、容差边界和拒绝样例。

订单簿：

- 单笔和多笔新增，买卖价排序。
- 同价位 FIFO 和聚合量。
- 活动期重复 `OrderKey`，以及全成/撤单后再次使用同一个 key。
- 未成交订单全撤。
- **先部分成交，再撤销全部剩余量。**
- 撤销隐藏 `Aggressive` 订单时活动状态删除、可见价位不变。
- 未知撤单及其失败原子性。
- 单侧和双侧成交、部分成交、全部成交。
- 部分成交不改变 FIFO 位置。
- 成交过量。
- 严格/兼容未知成交引用。
- 空本方/对手盘的最优价委托。
- 兼容窗口内外的穿越订单、隐藏状态和剩余量重新入簿。
- 成交统计及 checked overflow。
- 每类失败前后的完整状态快照相等。
- 改变 `local_time` 只改变事件元数据，不改变价位、订单、成交或统计。
- `local_time`、`quote_time` 只在成功应用后推进，失败事件保持两者不变。
- `BookEvent`、`OrderBook` 和 `BookSummary` 均不包含 legacy `steady_time`。

replay：

- 两个空流、单边流、交错流和尾部排空。
- 任一流内乱序或重复。
- 跨流同序号歧义。
- 稀疏但严格递增的合法原始序号。
- 稠密、连续的应用序号。
- 首应用序号错误、重复、跳号和 `checked_add` 溢出。
- normalization/apply 失败后停止，不跳过序号。
- 不使用任何时间字段参与排序或解决同序号歧义。
- 生产 replay API 不暴露运行耗时字段。

属性测试：

- 随机合法事件序列后始终满足全部核心不变量。
- 与简单、低性能参考模型比较价位聚合和活动订单余量。
- `OrderKey` 在方向、通道和订单号各维度的碰撞隔离。

### 11.2 轻量 C++ oracle

旧项目没有可直接复用的订单簿测试和 fixture，完整 CMake 工程也不适合作为日常测试依赖。
因此只提取原订单簿相关源码，配合最小 shim 构建测试专用 oracle：

```text
tests/
├── cpp_oracle/
│   ├── 原订单簿相关源码或受控引用
│   ├── 最小类型 shim
│   └── oracle 主程序
└── fixtures/
    └── legacy_golden/
```

对拍流程：

1. 构造双方都定义良好的合法事件序列。
2. 可选命令使用 `g++` 编译并运行 oracle。
3. 将每步可见价位、FIFO、活动订单和统计输出为稳定文本格式。
4. 审核后把 golden 提交到仓库。
5. 日常 `cargo test` 只读取 golden，不要求开发机安装 C++ 工具链。
6. 需要时运行 live differential test 同时执行 Rust 与 C++。

C++ oracle 只用于锁定合法兼容子集。空盘口解引用、重复订单破坏、overfill 下溢等旧缺陷通过
Rust 负面测试验证，不生成“兼容” golden。旧实现允许终态后复用订单号，但严格 Rust
订单簿在同一个 `BookKey` 生命周期内拒绝复用；golden 不构造这类输入。

C++ golden 不比较 `steady_time`。`local_time`、`quote_time` 按 Rust 的“最后成功应用事件”
语义验收，不复刻旧 C++ 在输入尚未应用时就提前覆盖结果时间的行为。

## 12. 实施阶段

### 阶段 0：冻结行为与 fixture

- 建立 C++ 行为矩阵和 legacy fixture。
- 固定 `BookKey`、价格 scale、必需 `local_time`/`quote_time`、可选 legacy
  `steady_time` 的对应，以及订单引用 resolver。
- 建立轻量 C++ oracle 的最小构建。

出口：至少一组新增、成交、部分成交后全撤、隐藏订单和统计 golden。

### 阶段 1：领域类型与 normalization

- 实现强类型、`MarketDataRecord`、`BookEvent` 和错误类型。
- 在 `src/market_data/types.rs` 中移除三种时钟共用的 `TimestampNs`，将
  `steady_time` 改为 legacy raw 可选字段，并让 `local_time`、`quote_time` 使用不同的
  必需 newtype。
- 实现旧枚举、方向、标识、数量和价格转换。
- 实现 `LegacyQtpRules` 的明确映射。

出口：全部字段映射和非法输入测试通过。

### 阶段 2：订单与价位基础结构

- 实现 `SlotMap` 订单存储、活动索引、已见 key tombstone 和价位 FIFO。
- 实现只读 view 和 `check_invariants()`。
- 实现新增订单及状态依赖型价格解析。

出口：新增、排序、FIFO、聚合、重复和空盘口测试通过。

### 阶段 3：撤单与成交

- 实现撤销全部剩余量。
- 实现单/双侧部分成交、全部成交和档位清理。
- 实现隐藏穿越订单及剩余量重新入簿。
- 实现严格/兼容未知成交策略和成交统计。

出口：失败原子性、overfill、部分成交后全撤及统计测试通过。

### 阶段 4：双流 replay

- 实现两个有序流的归并、顺序校验和 `apply_sequence`。
- 实现跨流同序号错误和尾部排空。

出口：replay 全矩阵测试通过，结果与输入到达批次无关。

### 阶段 5：对拍与强化

- 生成并审核 C++ golden。
- 加入逐事件差分、属性测试和慢速参考模型。
- 记录所有有意行为差异。
- 添加基准，定位数据结构热点；只在 benchmark 外围使用 `std::time::Instant` 测量整段
  replay，不新增 `ReplayMetrics`，也不把运行耗时写入事件、订单簿状态或生产 replay API。
  本阶段不提前承诺吞吐数字。

出口：合法兼容样本逐事件一致，全部 Rust 负面和属性测试通过。

### 阶段 6：冻结 v1 API

- 收敛公共导出和文档示例。
- 执行格式、lint、单测、文档测试和可选 live differential。
- 评审后再决定通联 adapter、Clara 流程衔接或 Python 接口的下一阶段。

## 13. v1 完成标准

同时满足以下条件才认为订单簿恢复功能迁移完成：

- `BookEvent` 只有 `AddOrder`、`OrderCancel`、`Trade`。
- `MarketDataRecord::{Order, Trade}` 原始层保留且字段对应有测试。
- `local_time`、`quote_time` 是 raw 记录和核心事件中不同的必需类型。
- legacy `steady_time` 只存在于 raw 输入；`BookEvent`、`OrderBook` 和 `BookSummary` 均不
  包含它，改变其值不影响订单簿结果。
- 核心不存在 `CancelQuantity` 或部分撤单路径。
- 部分成交后撤单会删除全部剩余量。
- 所有错误都满足失败原子性。
- 每次成功事件后核心不变量成立。
- 双流归并校验顺序、处理同序号歧义并正确排空尾部。
- replay 只按 `raw_sequence` 排序，生产 API 不包含运行耗时字段。
- C++ 合法行为 golden 逐事件一致。
- C++ 已知缺陷有对应 Rust 负面测试。
- `cargo fmt --all -- --check` 通过。
- `cargo clippy --all-targets --all-features -- -D warnings` 通过。
- `cargo test --all-targets` 通过。
- 公共 API 不暴露内部 handle、可变容器或 `f64` 价格键。

## 14. 后续工作边界

完成 v1 后，通联文档可用于新增供应商 adapter，Clara 可用于构造原始数据 fixture 和核对清洗
假设。但新增 adapter 只能输出本方案定义的 `MarketDataRecord` 或 `BookEvent`，不得绕过
normalization 直接修改订单簿状态。

通联 adapter 的独立验收项包括：上交所合并逐笔、深市委托、深市成交均能解析必需
`LocalTime`；缺失、空值或非法值在创建 `MarketDataRecord` 前报错；禁止使用
`quote_time`、当前系统时间或零值代替。

Python、FFI、实时服务、多证券并行和持久化应分别立项，不纳入本次 C++ 订单簿迁移的完成
条件。

## 15. 生产 Parquet 链路（阶段 7）

核心 v1 冻结后，项目已增加独立的 `production` adapter 和 `qtp-replay` 程序。它不改变
legacy raw 类型，而是直接把通联字段转换为标准 `BookEvent`：

```text
Raw Parquet → 必要列投影/Schema 校验 → 股票过滤 → 通道临时分片
            → 沪深原生序号回放 → 每标的 OrderBook → 截面/验证
```

- 沪市 `mdl_4_24_0` 在每个 `Channel` 内按 `BizIndex` 回放 A/D/T，S 只触发阶段检查点。
- 深市 `mdl_6_33_0` 与 `mdl_6_36_0` 在每个 `ChannelNo` 内按 `ApplSeqNum` 归并。
- 普通截面严格为成功应用全部 `quote_time < T` 事件后的状态；收盘状态使用独立
  `MarketClose` 类型。
- 原始 Decimal128 直接转换为万分之一价格单位；必需的 `LocalTime` 与行情时间组合交易日
  和 Asia/Shanghai 时区后进入核心元数据。
- snapshot 只用于验证，不参与恢复。沪市三个锚点均使用 raw `MarketData`，分别选择此前存在
  `OCALL` 的首个 PreOpen `TRADE`、09:30 后的全部 `TRADE`、此前存在 `CCALL` 的首个
  `CLOSE`；深市三个锚点均使用 raw `mdl_6_28_0`，分别选择此前存在
  `O0` 的首个 `B0`、全部 `[09:30, 14:57)` `T0` 和首个 `E0`。PreOpen 与连续交易帧
  分别在其 `[ts, ts + 1s)` 秒桶内候选匹配，`CLOSE/E0` 比较严格定义的收盘状态。三个
  阶段统一比较十档价量和委托数、两侧总量、最新/最高/最低价及成交统计；加权委托均价
  同样比较，但允许绝对误差 `<= 0.001` 元，其余字段精确比较。深市后续重复 `E0` 必须
  与首帧全字段相同。深市股票和 ETF 的 E0 若仅 `LastPrice` 不同，则先确认收盘集合竞价
  没有成交，再用最后一笔成交前一分钟 VWAP（当日无成交则用 `PreCloPrice`）验证官方
  收盘价；只有独立计算结果一致才按带标签的匹配处理。验证报告的时间字段使用毫秒精度。

生产命令、目录布局、失败分片和 JSON 报告详见
[沪深全市场 Parquet 回放与验证手册](full-market-replay-guide.md)。
