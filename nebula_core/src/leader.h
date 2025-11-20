#pragma once

#include <boost/asio.hpp>
#include <mutex>
#include <optional>
#include <string>

#include "cluster.h"

namespace nebula {

class LeaderElector {
public:
    LeaderElector(boost::asio::io_context& io,
                  GossipCluster& cluster,
                  const std::string& self_id);

    void start();
    void stop();

    // Returns currently known leader id (may be empty early on)
    std::optional<std::string> leader_id() const;

    bool is_leader() const;

private:
    void schedule_tick();
    void tick();

    boost::asio::io_context& io_;
    GossipCluster& cluster_;
    std::string self_id_;

    mutable std::mutex mtx_;
    std::optional<std::string> current_leader_;

    boost::asio::steady_timer timer_;
    bool running_;
};

}  // namespace nebula
