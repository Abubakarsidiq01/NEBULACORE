#pragma once

#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <optional>

#include <boost/asio.hpp>

#include "cluster.h"
#include "leader.h"
#include "topic_manager.h"

namespace nebula {

struct NebulaNodeConfig {
    std::string data_root;
    NodeId self;
    std::vector<NodeId> peers;
    std::size_t default_partitions{3};
};

class NebulaNode {
public:
    explicit NebulaNode(const NebulaNodeConfig& cfg);

    void start();
    void stop();

    bool is_leader() const;
    std::optional<std::string> leader_id() const;

    std::pair<std::size_t, uint64_t> publish(
        const std::string& topic,
        const std::string& key,
        const std::string& payload);

    std::optional<std::string> consume(
        const std::string& topic,
        const std::string& consumer_group,
        std::size_t partition_index);

    const NebulaNodeConfig& config() const { return cfg_; }

private:
    NebulaNodeConfig cfg_;

    boost::asio::io_context io_;

    using WorkGuard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;
    std::optional<WorkGuard> work_guard_;

    std::thread io_thread_;

    std::unique_ptr<GossipCluster> cluster_;
    std::unique_ptr<LeaderElector> elector_;
    std::unique_ptr<TopicManager> topics_;

    void run_io();
};

}  // namespace nebula
