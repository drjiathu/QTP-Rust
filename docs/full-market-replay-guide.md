# 沪深全市场 Parquet 回放与验证手册

本手册说明当前命令与运行行为。目标匹配规则统一维护于
[Snapshot 验证匹配规则](snapshot-validation-rules.md)，其中的实现状态表列出尚待落实项。
特别是深市“完整单日回放后比较 E0”尚未替换当前 15:00 时间档收盘边界；下述命令不代表
已经启用该目标行为。

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
`snapshot_kind=market_close`：沪市由 `CLOSE` 状态触发，深市在完整处理 15:00:00 时间档后
触发。

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

每只可比证券验证：

- `pre_open`：直接从 raw 快照选择集合竞价结束后帧；沪市使用
  `MarketData` 中 `[09:25, 09:30)` 内、此前存在 `OCALL` 的首个 `TRADE`，深市使用
  `mdl_6_28_0` 中 `[09:25, 09:30)` 的首个正常 `B0`，并要求此前存在 `O0`；
  深市 `H0/B1` 仅作为停牌后备，完整比较订单簿与成交统计；
- `continuous_trading`：沪市使用 raw `MarketData` 中 09:30 后的全部 `TRADE` 帧；深市使用 raw
  `mdl_6_28_0` 中 `[09:30, 14:57)` 的全部正常 `T0` 帧，每帧分别验证；
- `market_close`：沪市取 raw `MarketData` 中此前存在 `CCALL` 的第一条 `CLOSE`；深市取 raw `mdl_6_28_0`
  的首个 `E0`，与处理完 `quote_time <= 15:00:00.000` 全部事件后的状态完整精确比较。
  raw 中同标的后续重复 `E0` 必须与首帧全字段相同，否则验证在加载阶段失败。

深市股票和 ETF 的 `E0.LastPrice` 均按深交所收盘价规则验证。默认仍直接比较全部字段；仅当
唯一差异为 `LastPrice` 时检查逐笔成交：若 `[14:57:00, 15:00:00]` 存在成交，不允许
替换；若该时段没有成交，则以最后一笔成交为终点，计算含首尾端点的 60 秒成交量加权平均价，
股票按 `0.01` 元、ETF 按 `0.001` 元取整后再比较；当日没有成交时使用 raw E0 的
`PreCloPrice`。独立计算值与 E0 一致才判为匹配，分别记录
`SZ_STOCK_AVG_CLOSE_PRICE`、`SZ_ETF_AVG_CLOSE_PRICE`、`SZ_STOCK_PRE_CLOSE_PRICE` 或
`SZ_ETF_PRE_CLOSE_PRICE` 标签。盘口或其他统计字段仍有差异时不会放宽。

两市验证均不再读取 canonical snapshot，也不再提供 `--reference-root`。沪市 `OCALL`、
`CCALL` 只用于确认阶段先后，`SUSP/ENDTR` 等状态不生成订单簿对拍记录；深市 `O0` 只用于
确认首个正常 `B0` 的阶段先后。`CCALL/C0` 属于收盘集合竞价，不作为连续交易订单簿参考。
参考快照的整秒时间是阶段时间，不是逐笔
`quote_time`。`hh:mm:ss.000` 表示该整秒内某个未公开的采样时刻，而非严格的秒起点状态。
沪市 `continuous_trading` 默认在 `[ts-1s, ts+1s)` 内枚举状态；深市默认使用
`[ts, ts+1s)`。20260828 实测表明深市主板股票使用默认范围即可精确匹配；ETF 在接入
复牌集合竞价语义后也使用此范围，
但创业板 `T0` 需要覆盖完整三秒帧，可用 `--continuous-lookahead 3s` 诊断
`[ts,ts+3s)`；`--continuous-lookback 1s` 可显式增加左侧一秒。两个参数只改变验证候选，
不改变逐笔回放和生产截面语义。
raw `UpdateTime` 是整秒阶段时间，因此 `pre_open` 也在 `[ts, ts+1s)` 内逐事件寻找候选；
`market_close` 只比较严格定义的收盘状态。三个阶段统一完整比较十档价格、数量和委托数、
两侧总量及加权均价、最新/最高/最低价、成交笔数、成交量和成交额。除加权均价外均使用
整数值精确比较；买卖加权均价先按参考 feed 的发布精度由全书深整数价量计算，再允许
绝对误差 `<= 0.001` 元（内部四位价格尺度的 10 个单位）。一侧为空时，双方都必须为空，
不能用容差把缺失值与数值视为相等。验证不使用 `LocalTime` 或另一 feed 的序号对齐。

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
记录始终完整保留。小样本需要检查每条 `matched_candidate_time_ms` 时可增加
`--retain-matched-records`。

深市停牌状态按锚点处理：正常 PreOpen 只允许 `O0 → B0`，正常连续交易只允许 `T0`，
正常收盘只允许 `E0`。`B1/T1/E1` 等全天停牌状态生成带具体原因的 `NotComparable`；
临时 `H0` 只使对应的 PreOpen 不可比，复牌后的 `T0` 与最终 `E0` 仍接受正常规则验证。
验证不会因为出现 `H0` 而丢弃后续逐笔事件，也不会把真实不一致静默移出分母。

## 流式与失败恢复

输入文件各扫描一次，按原生通道写入唯一临时目录，再逐通道恢复。成功后自动删除临时
分片；失败时错误会报告保留目录，便于排查。输出按 RecordBatch 批量写入，不累计全市场
截面。普通 `replay` 的峰值内存主要由一个输入 batch、当前通道全部活动订单以及一个输出
batch 构成。`validate` 当前还会加载当日全部 reference 帧并保存逐帧匹配状态，20260828
全市场实测峰值约 22 GiB；它是离线验收命令，尚未达到同样的通道级内存边界。

发生失败时不要把保留分片当作新的输入继续运行；修复源数据或规则后重新执行命令。已有的
部分输出可能不完整，应写到新的输出目录或先人工确认后处理。

## 开发验证

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-targets
cargo test --locked --doc
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
```

CI 使用 `tests/production_parquet.rs` 中的小型 Arrow/Parquet fixture，不依赖 `/hdd`。
