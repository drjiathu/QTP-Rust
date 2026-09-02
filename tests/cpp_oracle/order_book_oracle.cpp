// Standalone legal-subset oracle extracted from QTP-core-main OrderBook.cpp.
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <list>
#include <map>
#include <sstream>
#include <string>

namespace {

enum class Side { Buy, Sell };

struct Order {
  std::int64_t id{};
  Side side{};
  std::int64_t price{};
  std::uint64_t volume{};
  std::uint64_t traded{};
  bool visible{};
};

struct Level {
  std::uint64_t volume{};
  std::list<std::int64_t> orders;
};

class Oracle {
 public:
  void Add(std::int64_t id, Side side, std::int64_t price,
           std::uint64_t volume, bool hide_if_crossing) {
    Order order{id, side, price, volume, 0, true};
    if (hide_if_crossing && Crosses(side, price)) order.visible = false;
    orders_.emplace(id, order);
    if (order.visible) Attach(id);
  }

  void Cancel(std::int64_t id) {
    const auto order = orders_.at(id);
    if (order.visible) Detach(order);
    orders_.erase(id);
  }

  void Trade(std::int64_t bid_id, std::int64_t ask_id, std::int64_t price,
             std::uint64_t volume) {
    last_ = price;
    if (!has_trade_) {
      low_ = high_ = price;
      has_trade_ = true;
    } else {
      low_ = std::min(low_, price);
      high_ = std::max(high_, price);
    }
    total_volume_ += volume;
    turnover_ += static_cast<unsigned long long>(price) * volume;
    ++trade_count_;

    Reduce(bid_id, volume);
    Reduce(ask_id, volume);
    FinishOrReenter(bid_id);
    FinishOrReenter(ask_id);
  }

  std::string Snapshot(const std::string& label) const {
    std::ostringstream out;
    out << label << "|last=" << (has_trade_ ? std::to_string(last_) : "-")
        << "|range=";
    if (has_trade_)
      out << low_ << "/" << high_;
    else
      out << "-/-";
    out << "|qty=" << total_volume_ << "|turnover=" << turnover_
        << "|trades=" << trade_count_ << "|bids=" << BidLevels()
        << "|asks=" << AskLevels() << "|active=" << Active() << "\n";
    return out.str();
  }

 private:
  bool Crosses(Side side, std::int64_t price) const {
    if (side == Side::Buy)
      return !asks_.empty() && price >= asks_.begin()->first;
    return !bids_.empty() && price <= bids_.rbegin()->first;
  }

  void Attach(std::int64_t id) {
    auto& order = orders_.at(id);
    auto& level =
        order.side == Side::Buy ? bids_[order.price] : asks_[order.price];
    level.volume += order.volume - order.traded;
    level.orders.push_back(id);
    order.visible = true;
  }

  void Detach(const Order& order) {
    auto& levels = order.side == Side::Buy ? bids_ : asks_;
    auto& level = levels.at(order.price);
    level.volume -= order.volume - order.traded;
    level.orders.remove(order.id);
    if (level.volume == 0) levels.erase(order.price);
  }

  void Reduce(std::int64_t id, std::uint64_t volume) {
    if (id == 0 || orders_.count(id) == 0) return;
    auto& order = orders_.at(id);
    if (order.visible) {
      auto& levels = order.side == Side::Buy ? bids_ : asks_;
      auto& level = levels.at(order.price);
      level.volume -= volume;
      if (order.traded + volume == order.volume) {
        level.orders.remove(id);
        if (level.volume == 0) levels.erase(order.price);
      }
    }
    order.traded += volume;
  }

  void FinishOrReenter(std::int64_t id) {
    if (id == 0 || orders_.count(id) == 0) return;
    auto& order = orders_.at(id);
    if (order.traded == order.volume) {
      orders_.erase(id);
      return;
    }
    if (!order.visible) {
      const bool can_reenter =
          order.side == Side::Buy
              ? asks_.empty() || order.price < asks_.begin()->first
              : bids_.empty() || order.price > bids_.rbegin()->first;
      if (can_reenter) Attach(id);
    }
  }

  void FormatLevel(std::ostringstream& out, std::int64_t price,
                   const Level& level, bool& first_level) const {
    if (!first_level) out << ";";
    first_level = false;
    out << price << ":" << level.volume << "[";
    bool first_order = true;
    for (const auto id : level.orders) {
      if (!first_order) out << ",";
      first_order = false;
      const auto& order = orders_.at(id);
      out << id << ":" << order.volume - order.traded;
    }
    out << "]";
  }

  std::string BidLevels() const {
    if (bids_.empty()) return "-";
    std::ostringstream out;
    bool first_level = true;
    for (auto iterator = bids_.rbegin(); iterator != bids_.rend(); ++iterator)
      FormatLevel(out, iterator->first, iterator->second, first_level);
    return out.str();
  }

  std::string AskLevels() const {
    if (asks_.empty()) return "-";
    std::ostringstream out;
    bool first_level = true;
    for (const auto& [price, level] : asks_)
      FormatLevel(out, price, level, first_level);
    return out.str();
  }

  std::string Active() const {
    if (orders_.empty()) return "-";
    std::ostringstream out;
    bool first = true;
    for (const auto& [id, order] : orders_) {
      if (!first) out << ",";
      first = false;
      out << id << ":" << order.volume - order.traded << ":"
          << (order.visible ? "R" : "A");
    }
    return out.str();
  }

  std::map<std::int64_t, Order> orders_;
  std::map<std::int64_t, Level> bids_;
  std::map<std::int64_t, Level> asks_;
  bool has_trade_{};
  std::int64_t last_{};
  std::int64_t low_{};
  std::int64_t high_{};
  std::uint64_t total_volume_{};
  unsigned long long turnover_{};
  std::uint64_t trade_count_{};
};

}  // namespace

int main() {
  Oracle regular;
  regular.Add(101, Side::Buy, 100000, 100, false);
  std::cout << regular.Snapshot("R1");
  regular.Add(102, Side::Buy, 100000, 50, false);
  std::cout << regular.Snapshot("R2");
  regular.Add(201, Side::Sell, 101000, 120, false);
  std::cout << regular.Snapshot("R3");
  regular.Trade(101, 201, 100500, 40);
  std::cout << regular.Snapshot("R4");
  regular.Cancel(102);
  std::cout << regular.Snapshot("R5");
  regular.Trade(101, 201, 100700, 60);
  std::cout << regular.Snapshot("R6");
  regular.Cancel(201);
  std::cout << regular.Snapshot("R7");

  Oracle hidden;
  hidden.Add(301, Side::Sell, 100000, 100, false);
  std::cout << hidden.Snapshot("H1");
  hidden.Add(302, Side::Buy, 101000, 150, true);
  std::cout << hidden.Snapshot("H2");
  hidden.Trade(302, 301, 100000, 100);
  std::cout << hidden.Snapshot("H3");
}
