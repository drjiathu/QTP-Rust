# 订单簿恢复实现说明

本文描述当前实现，不另立验收规则。运行方法见[使用手册](Guidance.md)，
已验证版本见[验证基线](real-data-validation.md)，匹配契约见[验收规范](snapshot-validation-rules.md)。

## 模块与调用关系

```text
qtp-replay / Rust 调用者
├─ replay_market_day(MarketDayRequest)
│  └─ run_market_day + 空观察器
├─ validate_market_day(ValidationConfig)
│  └─ load_references → run_market_day + ValidationObserver → ValidationReport
└─ OrderBook::apply(BookEvent)
   └─ 自定义源调用者直接提供标准事件
                         ↓
                      BookEvent → OrderBook
```

生产 `run_market_day` 内部依次执行：输入校验和通道分片 → 必要的深市逆序修复 →
按通道回放 → 按证券分发标准事件 → 成功事件通知观察器／写截面 → 清理分片。
`validate` 在回放过程中检查候选，不是恢复全日后重新读取每个历史状态。

| 位置 | 职责和主要接口 |
| --- | --- |
| [CLI](../src/bin/qtp-replay.rs) | 请求构造、参数互斥、报告输出、退出码 |
| [production/types.rs](../src/production/types.rs) | `MarketDayRequest`、`ValidationConfig`、证券前缀分类与策略 |
| [input.rs](../src/production/input.rs)、[spool.rs](../src/production/spool.rs)、[sequence.rs](../src/production/sequence.rs) | Arrow 投影、Clara provenance、通道分片、自然有序段修复 |
| [production/replay.rs](../src/production/replay.rs) | `process_sse_channel/process_sz_channel`、引用解析、每证券 runtime、收盘检查点 |
| [sz_pending.rs](../src/production/sz_pending.rs) | 严格／诊断市价策略和空簿本方最优响应组 |
| [market_data](../src/market_data/mod.rs)、[order_book](../src/order_book/mod.rs) | 强类型事件、价位／订单状态机、统计及只读查询 |
| [snapshot.rs](../src/production/snapshot.rs)、[writer.rs](../src/production/writer.rs) | `SnapshotBookView`、`BookSnapshot`、时间网格和批量 Parquet 输出 |
| [validation.rs](../src/production/validation.rs)、[phases.rs](../src/production/validation/phases.rs)、[close_range.rs](../src/production/validation/close_range.rs) | 参考选帧、候选、重复静态帧、收盘比较视图和分类报告 |

## 共享核心与数据边界

- 一个 `BookKey { market, trading_day, symbol }` 对应一个 `OrderBook`；
  `OrderKey { channel_id, side, order_id }` 标识订单，同一簿生命周期禁止复用。
- `BookEvent::{AddOrder, OrderCancel, Trade}` 驱动状态；撤单删除全部未成交余量，
  成交只按源数量扣减，不主动模拟撮合。
- 两侧 `BTreeMap` 保存价格档；`SlotMap` 保存订单，档内双向链维持 FIFO。
  活动索引、历史引用和终身去重索引分别服务查询、解析和防止编号复用。
- 核心事件先校验再提交，overfill、未知引用或算术错误不得部分改变订单簿；
  `last_applied_meta` 仅在成功应用后更新。
- 生产价格乘数固定 `10_000`，Decimal 直接整数转换，成交额以 `u128` checked arithmetic
  累计。时间用不同的必需 local／quote newtype；核心和生产输入不携带 steady。
  local 只作审计，不参与排序。价格乘数的来源说明见[README](../README.md#价格单位)。
- 当前“实时恢复”指按逐笔更新状态；生产入口是完整单日文件的离线回放，
  不包含交易所网络接入、实时水位或跨通道全市场统一序号。

旧 QTP 兼容模块、raw 记录、steady 类型和切片续跑接口已退役。核心的 OppositeBest、
HideIfCrossing 保留为显式事件能力；UpdateKnownAndStatistics、RestAtLastTradePrice、
AlwaysHide 等仍由生产使用，不能随旧接口删除。C++ oracle 已移除，固定 golden 和
直接调用 OrderBook 的 Rust 回归测试保留，见[测试说明](../tests/fixtures/golden/README_CN.md)。

## 沪市

`mdl_4_24_0` 按 `Channel + BizIndex` 回放，`A/D/T/S` 分别处理：

- A：源数据已经表示剩余新增委托，按源价量直接入簿，使用 `Provided + Rest`。
- D：引用必须指向活动订单，删除全部余量。
- T：主动方允许尚无活动委托引用；被动方必须存在。N 类成交不豁免任一侧引用。
  adapter 先校验，再由核心更新已知订单与整笔成交统计。
- S：不进入 `BookEvent`，首个逐笔 CLOSE 触发收盘检查点，不修改最后成功事件元数据。

恢复检查点与参考选帧是两件事。ETF 的 20260706 日期分支属于 validation 参考阶段判断，
不改变上述逐笔处理，详见[收盘规范](snapshot-validation-rules.md#23-收盘阶段结束集合竞价或历史-etf-连续竞价)。

## 深市

### 双流顺序与偶发逆序

委托 `mdl_6_33_0`、成交／撤单 `mdl_6_36_0` 在同一 ChannelNo 内共用 ApplSeqNum。
两流分别严格有序后双路归并，再按证券分发；不能以每证券有序替代通道有序。

扫描时 `observe_sz_sequence` 在证券／时间过滤前记录相邻逆序；写所选事件时，
`NaturalRuns::observe` 记录临时分片的自然有序段逻辑行偏移。写入器 flush 关闭后，
`NaturalRuns::repair` 仅重写异常分片：独立 reader 读取各段，最小堆归并，最多同时
32 段，更多段分轮处理。正常单段流没有额外重排读写，原始 Parquet 不修改。

例如委托为 `1(A), 5(B), 2(A)`、成交为 `3(A), 4(A)`，先修复委托为
`1(A), 2(A), 5(B)`，双流归并得到 `1,2,3,4,5`；A/B 只是证券，不参与排序。

- 相邻同号失败；所选事件跨段重复在修复时失败，跨流同号在归并时失败。不去重、不用
  SecurityID、LocalTime、quote_time、SeqNo 或源行号打破平局；合法序号间隙允许存在。
- `sz_sequence_regressions` 保存过滤前逆序及源行号；
  `sz_sequence_repairs` 保存所选流的实际修复行数、段数和轮数。过滤后可能无需修复。
- 异常流 N 条记录、R 段，比较复杂度 `O(N log R)`、段元数据 `O(R)`；归并读缓冲
  最多约 8 MiB，另计 Arrow batch 和订单簿。一个逆序点也可能需要重写整条异常流，
  不是仅付出常数开销；多轮修复需额外磁盘。
- 原始字段和 `source_row_no` 保留，分片完成后的逆序审计写入
  `sequence-regressions.json`；失败分片保留。该机制只适用于完整离线输入。

### 限价、市价与本方最优

| OrdType | 当前生产行为 |
| --- | --- |
| `2` 限价（raw 50） | `Provided(source_price) + Rest`，仅实际成交／撤单改变余量；穿价不隐藏 |
| `1` 市价（raw 49） | CLI 默认 `Unpriced + RestAtLastTradePrice`，不以保护价入簿 |
| `U` 本方最优（raw 85） | 接收时一次取本方最优价；本方为空时严格对账源全量撤单 |

限价单允许逐事件中间盘口交叉；恢复不读取 raw snapshot 识别 H0/V0 或决定订单可见性。
`ExecType='F'`（raw 70）生成成交；`'4'`（raw 52）须单侧活动引用，且 LastQty 恰等于
当前余量，才生成全撤。未知引用、数量不符和 overfill 不静默跳过。

默认市价策略：订单先隐藏；该订单参与成交后，用这笔成交价更新隐藏余量的价格，
只有不再穿越对手盘或对手盘为空时才入簿。入簿后价格固定，后续成交扣量、撤单删余量；
不要求响应同毫秒或原生序号相邻，不识别所有市价子类型。

已核对的通联逐笔委托字段不含 TimeInForce、MaxPriceLevels、MinQty 或即时执行结束标记，
因此不能仅凭 OrdType 和单一成交价还原全部执行限定，也不能假定自动撤单必然相邻。

已知局限：IOC 等订单在成交与自动撤单之间可能临时可见，可能影响后续本方最优定价。
周期 snapshot 匹配不证明每个中间态、FIFO 或严格市价路径精确。

### 保留的严格与诊断策略

| SzMarketOrderPolicy | 选择方式与保证 |
| --- | --- |
| `RestAtLastTradePrice` | 深市 CLI 默认；当前全量基线采用的实用策略 |
| `RequireEvidence` | Rust 枚举 Default；CLI 显式选择。全成／全撤可闭合；余量无充分证据则失败 |
| `AssumeContiguous` | 显式诊断假设即时响应连续；不是已确认的通联契约 |

严格／诊断市价处理及空簿本方最优使用 PendingOrder：以证券、通道、方向、订单号关联，
要求原生序号相邻、同一 quote_time，最多 65,536 条响应；检查正数量、不过量和全撤余量。
这些是当前解析器限制，不是交易所协议保证；跨时间或缺口报 `UnresolvedSzOrder`。

严格模式不凭单一成交价推断余量。诊断模式还须：存在成交、全部同价、等于接收前
对手方最优价，且有紧接的下一条原生记录；EOF／缺口不算结束证据。空簿本方最优
必须由同毫秒相邻全撤闭合，不无限隐藏，不另造一笔撤单。

分类先于组内修改，原始事件依次保持核心失败原子性；观察器及截面只看到已完成组的
最终状态。组中失败终止通道，不承诺半组原地回滚恢复，先前输出仅是失败运行的部分结果。
显式余量入簿不造原始序号、不借下一条消息时间。将来取得执行限定或明确结束契约后
才能扩展确定性分支，不能仅放宽当前检查。

## 截面、验证和资源

生产截面使用严格 `quote_time < T`；验证候选窗与其独立。开启截面输出时拒绝行情时间
回退；未输出截面时，当前实现允许并审计同一秒内回退，跨秒回退仍失败，不改变原生顺序。
审计计数为 `same_second_quote_time_regressions`；这种容忍不用于固定间隔截面输出。

沪市收盘在逐笔 CLOSE 处捕获；深市完整耗尽通道两流后捕获，不按 15:00 截断。
深市边界为 `max(当日15:00, 已应用行情时间上界)`；15:00 后事件计入
`sz_after_close_events(_by_symbol)`，未经阶段确认阻断 E0 验收。

validation 单独加载 raw snapshot、检查阶段链与重复静态帧，并通过 StateObserver 接收
恢复状态。深市收盘 LastPrice 转换及无涨跌幅限制价格范围投影只作用于比较视图，
不改恢复账本／生产输出，不过滤或补写参考盘口。完整契约只见[验收规范](snapshot-validation-rules.md)。

replay 每次处理一个通道，内存含 batch、该通道订单簿及历史／去重索引、输出 batch，
不只由活动订单量决定。validate 还加载整日参考帧与匹配状态，未达到通道级内存边界。
`profiling` feature 只测量时间归属，不改变规则，详见[基线与计时](real-data-validation.md#profiling)。
