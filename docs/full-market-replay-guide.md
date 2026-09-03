# 沪深全市场 Parquet 回放与验证手册

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
snapshot/date=YYYYMMDD/market={SH|SZ}/part-0.parquet
```

沪市只读取 `mdl_4_24_0`；深市同时读取后两个文件。程序验证 Clara footer、字段类型和
Decimal scale，只投影回放需要的列。

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

## 官方 snapshot 验证

```bash
target/release/qtp-replay validate \
  --date 20260828 \
  --market SZ \
  --symbols 000001,300750 \
  --report reports/20260828-sz.json
```

每只可比证券验证：

- `pre_open`：`[09:25, 09:30)` 内保留帧；
- `continuous_end`：全部普通连续交易候选帧中时间最大的一条，沪市为 `TRADE`、深市为 `T0`；
- `market_close`：沪市唯一 `CLOSE`、深市唯一 `15:00:00 E0`。

`CCALL/C0` 属于收盘集合竞价帧，不作为连续交易订单簿参考。参考快照的 `ts` 是周期帧
时间，不是逐笔 `quote_time`；因此选最后一条 `TRADE/T0` 时不套用逐笔事件的 `<14:57`
截断规则。前两个锚点会在参考毫秒的左极限状态以及该毫秒每个成功事件后的状态中寻找精确匹配；
收盘锚点只比较严格定义的收盘状态。比较字段包括十档价量和委托数、两侧总量及加权均价、
最新/最高/最低价、成交笔数、成交量和成交额。验证不使用 `LocalTime` 或另一 feed 的序号
对齐。加权均价在比较前按参考 feed 的原生发布精度做整数舍入：沪市 0.001 元、深市
0.01 元；其余价格仍按万分之一单位精确比较。

JSON 报告包含 `matched`、`mismatched`、`not_comparable`、可比锚点匹配率、不可比覆盖率、
差异字段分布和逐证券差异。存在不一致、Schema 错误或缺少应有收盘帧时命令返回非零。

## 流式与失败恢复

输入文件各扫描一次，按原生通道写入唯一临时目录，再逐通道恢复。成功后自动删除临时
分片；失败时错误会报告保留目录，便于排查。输出按 RecordBatch 批量写入，不累计全市场
截面。峰值内存主要由一个输入 batch、当前通道全部活动订单以及一个输出 batch 构成。

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
