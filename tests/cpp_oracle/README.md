# Lightweight C++ oracle

`order_book_oracle.cpp` is a standalone extraction of the legal order-book
behaviour used by the original `QTP::Quote::Generato::OrderBook`. It deliberately
does not reproduce undefined or defective paths such as missing level
dereferences, unsigned overfill, or duplicate-order corruption.

The checked-in golden can be regenerated and compared without building the
original CMake project:

```bash
g++ -std=c++17 -O2 tests/cpp_oracle/order_book_oracle.cpp -o /tmp/qtp-order-book-oracle
/tmp/qtp-order-book-oracle > /tmp/qtp-order-book-golden.txt
diff -u tests/fixtures/legacy_golden/order_book.txt /tmp/qtp-order-book-golden.txt
```

Normal `cargo test` reads the reviewed golden and does not require a C++
toolchain.
