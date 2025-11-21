#include "nebula_node.h"

#include <algorithm>
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
    std::size_t partition_index = local_res.first;
    uint64_t local_offset = local_res.second;

    // Collect offsets from followers
    std::vector<uint64_t> follower_offsets;
    follower_offsets.reserve(replication_peers_.size());
    for (NebulaNode* peer : replication_peers_) {
        if (!peer) continue;
        try {
            auto peer_res = peer->replicate_publish(topic, key, payload);
            // peer_res.first should equal partition_index if your hashing is deterministic
            follower_offsets.push_back(peer_res.second);
        } catch (const std::exception& ex) {
            std::cerr << "Replication failed on peer: " << ex.what() << "\n";
        }
    }

    // Now compute commit index for this partition.
    // We have one local offset and N follower offsets.
    // Total nodes = 1 leader + follower_offsets.size()
    const std::size_t total_nodes = follower_offsets.size() + 1;
    const std::size_t quorum = (total_nodes / 2) + 1;

    // Combine all offsets including leader
    std::vector<uint64_t> all_offsets;
    all_offsets.reserve(total_nodes);
    all_offsets.push_back(local_offset);
    for (auto off : follower_offsets) {
        all_offsets.push_back(off);
    }
    std::sort(all_offsets.begin(), all_offsets.end());

    // Commit index is the value at position quorum minus one
    uint64_t new_commit = all_offsets[quorum - 1];

    PartitionKey key_partition{topic, partition_index};
    auto it = commit_index_.find(key_partition);
    if (it == commit_index_.end()) {
        commit_index_[key_partition] = new_commit;
    } else {
        if (new_commit > it->second) {
            it->second = new_commit;
        }
    }

    return local_res;
}

std::optional<std::string> NebulaNode::consume(
    const std::string& topic,
    const std::string& consumer_group,
    std::size_t partition_index) {

    return topics_->read_next(topic, consumer_group, partition_index);
}

uint64_t NebulaNode::committed_offset(const std::string& topic,
                                      std::size_t partition) const {
    PartitionKey key{topic, partition};
    auto it = commit_index_.find(key);
    if (it == commit_index_.end()) {
        return 0;
    }
    return it->second;
}

}  // namespace nebula
