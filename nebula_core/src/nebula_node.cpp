#include "nebula_node.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace fs = std::filesystem;

namespace nebula {

NebulaNode::NebulaNode(const NebulaNodeConfig& cfg)
    : cfg_(cfg),
      io_() {

    fs::create_directories(cfg_.data_root);

    topics_ = std::make_unique<TopicManager>(cfg_.data_root);
    cluster_ = std::make_unique<GossipCluster>(io_, cfg_.self, cfg_.peers);
    elector_ = std::make_unique<LeaderElector>(io_, *cluster_, cfg_.self.id);

    // Keep io_context alive
    work_guard_.emplace(boost::asio::make_work_guard(io_));
}

void NebulaNode::run_io() {
    try {
        io_.run();
    } catch (const std::exception& ex) {
        std::cerr << "NebulaNode io_context exception: " << ex.what() << "\n";
    }
}

void NebulaNode::start() {
    cluster_->start();
    elector_->start();

    io_thread_ = std::thread([this]() { run_io(); });
}

void NebulaNode::stop() {
    cluster_->stop();
    elector_->stop();

    if (work_guard_.has_value()) {
        work_guard_.reset();
    }

    io_.stop();

    if (io_thread_.joinable()) {
        io_thread_.join();
    }
}

bool NebulaNode::is_leader() const {
    return elector_->is_leader();
}

std::optional<std::string> NebulaNode::leader_id() const {
    return elector_->leader_id();
}

void NebulaNode::set_replication_peers(const std::vector<NebulaNode*>& peers) {
    replication_peers_ = peers;
}

/*
    Follower append:
    - No leader checks
    - Followers simply append to their local log
*/
std::pair<std::size_t, uint64_t> NebulaNode::replicate_publish(
    const std::string& topic,
    const std::string& key,
    const std::string& payload) {

    TopicConfig cfg;
    cfg.name = topic;
    cfg.num_partitions = cfg_.default_partitions;
    topics_->create_topic(cfg);

    return topics_->publish(topic, key, payload);
}

/*
    Leader publish:
    - Append locally
    - Replicate to followers
    - Require majority acknowledgement
*/
std::pair<std::size_t, uint64_t> NebulaNode::publish(
    const std::string& topic,
    const std::string& key,
    const std::string& payload) {

    if (!is_leader()) {
        throw std::runtime_error("publish called on non leader node");
    }

    TopicConfig cfg;
    cfg.name = topic;
    cfg.num_partitions = cfg_.default_partitions;
    topics_->create_topic(cfg);

    // Leader applies locally
    auto local_res = topics_->publish(topic, key, payload);
    std::size_t successes = 1;  // leader itself

    const std::size_t total_nodes = replication_peers_.size() + 1;
    const std::size_t majority = total_nodes / 2 + 1;

    // Replicate to followers
    for (NebulaNode* peer : replication_peers_) {
        if (!peer) {
            continue;
        }
        try {
            peer->replicate_publish(topic, key, payload);
            successes += 1;
        } catch (const std::exception& ex) {
            std::cerr << "Replication failed on peer: " << ex.what() << "\n";
        }
    }

    if (successes < majority) {
        throw std::runtime_error("replication did not reach majority");
    }

    return local_res;
}

std::optional<std::string> NebulaNode::consume(
    const std::string& topic,
    const std::string& consumer_group,
    std::size_t partition_index) {

    return topics_->read_next(topic, consumer_group, partition_index);
}

}  // namespace nebula
