# Fixed order-book golden

[中文版](README_CN.md)

This directory contains reviewed, fixed expected output for the two independent
Rust tests in [tests/golden.rs](../../golden.rs). Each test uses `run_scenario()`
to apply `BookEvent` values to the production `OrderBook` core, then compares the
recorded states for every step with its scenario file.

The expected results originated from a standalone C++ program during migration.
That program and its CI job have been removed. The former combined fixture in
`legacy_golden/order_book.txt` is now split by scenario under `golden/`, with event
sequences, checkpoint labels and expected values unchanged. These tests call the
core directly and do not depend on a legacy input API.
Historical C++ source remains available in Git history, including `73adb97`.

## Coverage and limits

- [basic_lifecycle.txt](basic_lifecycle.txt), R1-R7: additions, same-price FIFO,
  partial/full fills, full-remainder cancels, empty-book cleanup and retained
  trade statistics.
- [hidden_reentry.txt](hidden_reentry.txt), H1-H3: explicit
  `Provided + HideIfCrossing`, followed by hidden remainder re-entry at the
  original order price. This is not `RestAtLastTradePrice`.
- Each checkpoint compares visible levels, aggregate quantity, FIFO quantities,
  selected active orders and resting/hidden state, plus last/high/low price,
  trade count, volume and turnover. Prices use 10,000 integer units per yuan.

There are two synthetic scenarios and ten checkpoints, not exchange matching
simulations. They do not test execution-price eligibility, event timestamps,
invalid-input atomicity, production adapters or snapshot validation. Core unit
tests, property tests and production fixtures provide complementary coverage.

## Scenario summaries

### R: basic lifecycle (`Provided + Rest`)

| Checkpoint | Input event | Expected state |
| --- | --- | --- |
| R1 | Add buy 101: CNY 10.00 × 100 shares | Bid level: 100 shares |
| R2 | Add buy 102: CNY 10.00 × 50 shares | Bid level: 150 shares; FIFO: 101 → 102 |
| R3 | Add sell 201: CNY 10.10 × 120 shares | Ask level: 120 shares; bids unchanged |
| R4 | Inject a trade between 101 and 201: CNY 10.05 × 40 shares | Remaining: 101 = 60, 102 = 50, 201 = 80 shares |
| R5 | Cancel buy 102 | Remove its remaining 50 shares; bid level contains only 101 with 60 shares |
| R6 | Inject a trade between 101 and 201: CNY 10.07 × 60 shares | Remove fully filled 101; 201 has 20 shares left; cumulative volume: 100 shares |
| R7 | Cancel sell 201 | Empty book; retain two trades, 100 shares and CNY 1,006.20 turnover |

The buy and sell limits do not cross; these injected trades are not valid
limit-order executions. This historical synthetic fixture checks state updates
from supplied order, trade and cancel events, not matching decisions or execution
eligibility. The core does not generate trades itself.

### H: hidden remainder re-entry (`Provided + HideIfCrossing`)

| Checkpoint | Input event | Expected state |
| --- | --- | --- |
| H1 | Add sell 301: CNY 10.00 × 100 shares, using `Provided + Rest` | Ask level displays 100 shares |
| H2 | Add buy 302: CNY 10.10 × 150 shares, using `Provided + HideIfCrossing` | Crosses the ask; 302 remains active with 150 shares but no visible bid level |
| H3 | Inject a trade between 302 and 301: CNY 10.00 × 100 shares | Remove 301; 302's remaining 50 shares rest at the original CNY 10.10 price, not the trade price |

H tests an explicitly selected core policy, not the production limit-order rule:
SH/SZ production limit orders use `Provided + Rest`, without inferred crossing
hiding. Other policies require their own cases; some are already covered in
[core tests](../../order_book.rs), so a missing golden is not missing test coverage.

## Run and maintain

```bash
cargo test --locked --test golden
```

Normal `cargo test` and the Rust CI test job include this check. No C++ compiler
or reference-output generation step is required.

Keep the expected file independent of the implementation under test. For an
intentional behavior change, review the event sequence and calculate each changed
expected state before editing the fixture. Do not regenerate it from `OrderBook`
merely to make a failing test pass. The removed C++ cross-check is no longer part
of this test; the fixed assertions remain.
