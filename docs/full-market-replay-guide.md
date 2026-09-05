# 沪深全市场 Parquet 回放与验证手册

本手册说明当前命令、运行行为和用户可见限制。验收要求只在
[Snapshot 验证匹配规则](snapshot-validation-rules.md) 中定义；本手册不改变或放宽该规范。
开发进度不写入规范文档，相关开发任务应通过 GitHub Issue 跟踪。

## 编译

项目固定 Rust 1.85.1，生产命令建议使用 release 构建：

```bash
rustup show
cargo build --release --locked --bin qtp-replay
target/release/qtp-replay --help
```

默认原始数据根目录为 `/hdd/data/stock/raw_level2_parquet`，参考快照根目录为
`/hdd/data/stock/snapshot`。输入文件布局必须为：

```text
raw_level2_parquet/date=YYYYMMDD/mdl_4_24_0/part-0.parquet
raw_level2_parquet/date=YYYYMMDD/mdl_6_33_0/part-0.parquet
raw_level2_parquet/date=YYYYMMDD/mdl_6_36_0/part-0.parquet
raw_level2_parquet/date=YYYYMMDD/mdl_6_28_0/part-0.parquet
snapshot/date=YYYYMMDD/market={SH|SZ}/part-0.parquet
```

沪市逐笔读取 `mdl_4_24_0`；深市逐笔读取 `mdl_6_33_0` 和 `mdl_6_36_0`。深市额外分批
投影 `mdl_6_28_0` 的 `SecurityID/UpdateTime/TradingPhaseCode/source_row_no`，识别每标的
`H0 → 首个 T0` 复牌检查点。检查点同毫秒发布的限价委托采用集合竞价 `Rest`，下一毫秒
恢复普通连续竞价规则；不硬编码证券或10:30。价格、数量和成交统计仍全部来自逐笔。
程序验证 Clara footer、字段类型和 Decimal scale，只投影回放需要的列。

兼容仅有逐笔的旧输入时，缺少 `mdl_6_28_0` 可继续运行，但报告会标记
`sz_phase_source_available=false`，不能保证盘中复牌恢复正确。文件存在但 Schema、footer、
必需值或逐标的阶段时间顺序不合法时直接报错。正常生产应提供此状态来源；该逻辑是已核验
通联“复牌批次统一时间戳”的适配契约，不能用于推断缺失状态或任意新数据源的竞价边界。
报告另有 `sz_phase_rows`（状态扫描行数）、`sz_resumption_checkpoints`（检查点数）和
`sz_resumption_rest_orders`（按复牌规则成功应用的限价委托数），逐笔 `input_rows` 不含状态行。

## 回放

恢复沪市全部 A 股，并每 30 秒输出十档截面：

```bash
target/release/qtp-replay replay \
  --date 20260828 \
  --market SH \
  --snapshot-interval 30s \
  --depth 10
```

只恢复指定的多个深市股票并每 100 毫秒输出：

```bash
target/release/qtp-replay replay \
  --date 20260828 \
  --market SZ \
  --symbols 000001,300750 \
  --snapshot-interval 100ms \
  --output-root /data/qtp-snapshots \
  --temp-root /data/qtp-spool
```

省略 `--symbols` 表示该市场全部 A 股；省略 `--snapshot-interval` 表示只恢复和校验输入，
不写截面。支持的 duration 后缀是 `ms`、`s`、`m`，因此 100 毫秒必须写为 `100ms`。
增加 `--include-etfs` 表示同时选择全部股票和 ETF；`--only-etfs` 只选择 ETF，且与
`--symbols`、`--include-etfs` 互斥。v1 使用代码前缀分类，规则位于
`src/production/types.rs`；尚未接入证券主数据。

输出布局为：

```text
OUTPUT/date=YYYYMMDD/market={SH|SZ}/channel=CHANNEL/part-0.parquet
```

普通截面满足严格语义：

```text
Snapshot(T) = 成功应用全部 quote_time < T 事件后的状态
```

遇到 `quote_time == T` 的事件时先写 `T`，再应用该事件。时间网格分别从 09:15 和
13:00 开始，第一帧为窗口起点加一个 interval；午间不输出重复静态帧。每只证券另写一条
`snapshot_kind=market_close`：沪市由逐笔 `CLOSE` 状态触发；深市在对应通道委托、成交／撤单
两路全部读完并成功应用后触发，不以 15:00 截断输入。

深市 `boundary_time` 为 `max(当日15:00, 已应用行情时间上界)`，表示离线最终状态的逻辑
边界，不是 snapshot E0 时间或程序执行耗时。若包含 15:00 后事件，该状态需要业务阶段
复核，不能仅凭 `snapshot_kind=market_close` 宣称等同于 E0。报告中的
`sz_after_close_events` 及 `sz_after_close_events_by_symbol` 分别给出成功应用的超时事件
总数和逐标的计数（严格 `quote_time >15:00:00.000`，不按 LocalTime 判断）。

价格、加权均价和成交额以 Decimal128(scale=4) 输出；逐笔 Decimal128 直接缩放为整数，
不会经过 `f64`。运行结束会输出 JSON 报告，其中分别统计非股票行和未选中股票行。

## snapshot 验证

```bash
target/release/qtp-replay validate \
  --date 20260828 \
  --market SZ \
  --symbols 000001,300750 \
  --report reports/20260828-sz.json
```

只验证全市场 PreOpen 时，使用 `--pre-open-only`。该模式仍完整扫描原始文件做
Schema 和覆盖校验，但只分片、回放 `quote_time < 09:30` 的事件：

```bash
target/release/qtp-replay validate \
  --date 20260828 --market SH --include-etfs --pre-open-only \
  --report reports/20260828-sh-preopen.json
```

选帧、候选窗口、比较字段、深市收盘价和停牌处理只在
[Snapshot 验证匹配规则](snapshot-validation-rules.md) 中定义；本手册不复制规则正文。
验证直接读取 raw snapshot，不读取 canonical snapshot，也不提供 `--reference-root`。

当前命令有以下运行行为：

- `--continuous-lookback 1s`、`--continuous-lookahead 3s` 只调整盘中验证候选窗口，
  不改变原生序号回放或生产截面。显式覆盖窗口会标记 `diagnostic_window_override`；
  即使全部匹配也不是标准验收。正式全量验收不要传入这两个参数。
- 深市收盘在通道内两路输入完全耗尽后比较 E0。若成功应用了
  `quote_time >15:00:00.000` 的事件，回放仍完成，但对应收盘项以
  `phase review required` 阻断验收；JSON 的 `sz_after_close_events` 和
  `sz_after_close_events_by_symbol` 保留总数与逐标的计数。
- `--pre-open-only` 不生成收盘检查点，也不把未应用的下午事件计入超时事件统计。

### 当前实现状态与限制

以下内容描述当前 `qtp-replay` 的实现，不重复定义或改变验收规范：

- 已实现按市场、证券板块自动选择标准窗口，同一深市请求可以混合主板、创业板和 ETF。
  `continuous_lookahead_ms_by_symbol` 是逐证券生效值；原标量字段只表示基础窗口。
- 已实现参考源阶段状态机、异常中间阶段检查、首次 T0 前的正常开盘选择，以及参考时间
  回退、来源行号重复／倒序诊断；`phase_audit` 保留全量状态计数和首个异常上下文。
- 已实现正常开盘重复 B0、重复 CLOSE/E0 的规范化字段精确一致性检查；冲突不会通过
  挑选另一帧消除，也不使用盘口对拍的加权均价容差。
- 结果分为 `Matched`、`Mismatched`、`ExcludedByStatus`、`DataError`、`MissingSource`。
  后两者均阻断验收；`NotComparable` 仅保留旧报告反序列化兼容。原 `not_comparable`
  汇总为后三类的合计，不代表这些记录通过了验收。
- 候选明细输出毫秒时间、通道、最后成功应用事件的 `matched_candidate_raw_sequence`
  和 `matched_candidate_apply_sequence`。静态延续候选的时间可能晚于该事件；空簿尚无
  成功事件时序号为空。失败明细保留差异最少候选的时间、原生序号及字段差异。
- 深市完整 EOF 收盘保留；15:00 后事件以 `DataError` 阻断未经业务阶段确认的 E0 验收。
- 功能回归已补入合成测试，97 项测试及严格 Clippy、格式／文档测试通过；20260828
  沪深股票与 ETF 的新规范全量验收已完成，全部正常可比项通过。分类明细与证据见
  [真实数据验证记录](real-data-validation.md)，旧报告不自动升级为本轮验收证据。

仍需跨日期回归验证窗口及复牌数据源契约；snapshot 汇总匹配不替代逐订单和 FIFO 测试。

JSON 报告包含 `matched`、`mismatched`、`not_comparable`、可比锚点匹配率、不可比覆盖率、
差异字段分布、`match_tags` 规则匹配统计和逐证券逐帧差异；保留的成功记录通过
`match_tag` 给出具体规则。`reference_time_ms` 与 `matched_candidate_time_ms` 都使用
Unix epoch 毫秒，忠实反映输入时间精度，不暗示纳秒级准确性；存在不一致、Schema 错误或
缺少应有收盘帧时命令返回非零。

全量诊断可使用 `--max-detail-records N` 限制 JSON 中保留的失败/不可比较明细。该选项不
截断 `total_anchors`、匹配/失败计数、差异字段统计或失败证券前缀统计；报告通过
`omitted_mismatched_records` 和 `omitted_not_comparable_records` 明示被省略的明细数。

全市场验证默认不把每条成功记录写入 `records`，但所有记录仍进入总数和
`stock/etf × anchor` 的 `breakdown`；`omitted_matched_records` 给出省略量。异常和不可比
记录默认完整保留，设置 `--max-detail-records` 时按上述规则限制明细。小样本需要检查每条 `matched_candidate_time_ms` 时可增加
`--retain-matched-records`。

## 流式与失败恢复

输入文件各扫描一次，按原生通道写入唯一临时目录，再逐通道恢复。成功后自动删除临时
分片；失败时错误会报告保留目录，便于排查。输出按 RecordBatch 批量写入，不累计全市场
截面。普通 `replay` 的峰值内存主要由一个输入 batch、当前通道订单簿（含历史引用与终身
去重索引）以及一个输出 batch 构成。`validate` 当前还会加载当日全部 reference 帧并保存
逐帧匹配状态；本轮 20260828 标准全量验收峰值约为沪市 36.9 GiB、深市 35.2 GiB。
它是离线验收命令，尚未达到同样的通道级内存边界。

发生失败时不要把保留分片当作新的输入继续运行；修复源数据或规则后重新执行命令。已有的
部分输出可能不完整，应写到新的输出目录或先人工确认后处理。

## 开发验证

提交前的格式、Clippy、测试、文档和依赖检查命令统一维护在
[CONTRIBUTING.md](../CONTRIBUTING.md#required-checks)。

CI 使用 `tests/production_parquet.rs` 中的小型 Arrow/Parquet fixture，不依赖 `/hdd`。
