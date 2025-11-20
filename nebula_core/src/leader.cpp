#include "leader.h"

#include <algorithm>
#include <iostream>

namespace nebula {

static constexpr int LEADER_TICK_MS = 400;  // how often we recompute leader

LeaderElector::LeaderElector(boost::asio::io_context& io,
                             GossipCluster& cluster,
                             const std::string& self_id)
    : io_(io),
      cluster_(cluster),
      self_id_(self_id),
      timer_(io),
      running_(false) {}

void LeaderElector::start() {
    if (running_) return;
    running_ = true;
    schedule_tick();
}

void LeaderElector::stop() {
    if (!running_) return;
    running_ = false;
    boost::system::error_code ec;
    timer_.cancel();  // no-arg cancel for modern Boost
}

void LeaderElector::schedule_tick() {
    if (!running_) return;

    timer_.expires_after(std::chrono::milliseconds(LEADER_TICK_MS));
    timer_.async_wait([this](const boost::system::error_code& ec) {
        if (!ec && running_) {
            tick();
            schedule_tick();
        }
    });
}

void LeaderElector::tick() {
    // pull current member snapshot from gossip
    auto members = cluster_.members_snapshot();
    if (members.empty()) {
        std::lock_guard<std::mutex> lock(mtx_);
        current_leader_.reset();
        return;
    }

    // pick lexicographically smallest id among alive members
    std::string candidate = members[0].id;
    for (const auto& m : members) {
        if (m.id < candidate) {
            candidate = m.id;
        }
    }

    {
        std::lock_guard<std::mutex> lock(mtx_);
        if (!current_leader_.has_value() || *current_leader_ != candidate) {
            current_leader_ = candidate;
        }
    }
}

std::optional<std::string> LeaderElector::leader_id() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return current_leader_;
}

bool LeaderElector::is_leader() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return current_leader_.has_value() && *current_leader_ == self_id_;
}

}  // namespace nebula
