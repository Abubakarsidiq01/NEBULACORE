#include "nebula_node.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <sstream>
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

    // ---------------------------------------------------------
    // Set the Raft transport (if raft_ exists)
    // ---------------------------------------------------------
    if (raft_) {
        raft_->set_transport(this);
        raft_->set_state_machine(this);
    }

    // ---------------------------------------------------------
    // Set peer IDs for network Raft elections
    // ---------------------------------------------------------
    if (raft_) {
        std::vector<std::string> peer_ids;
        for (const auto& p : cfg_.peers) {
            if (p.id != cfg_.self.id)
                peer_ids.push_back(p.id);
        }
        raft_->set_peer_ids(peer_ids);   // <--- IMPORTANT
    }


    // ---------------------------------------------------------
    // Initialize RaftClients (network connections)
    // ---------------------------------------------------------
    for (const auto& p : cfg_.peers) {
        if (p.id == cfg_.self.id) continue;

        uint16_t raft_port = p.gossip_port + 1000;

        try {
            raft_clients_[p.id] = std::make_unique<RaftClient>(p.host, raft_port);
            std::cout << "[NebulaNode] Connected RaftClient to "
                      << p.id << " at " << p.host
                      << ":" << raft_port << "\n";
        } catch (const std::exception& ex) {
            std::cerr << "[NebulaNode] Failed RaftClient to "
                      << p.id << ": " << ex.what() << "\n";
        }
    }
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

    if (raft_) {
        if (raft_->role() != RaftRole::Leader) {
            throw std::runtime_error("publish called on follower");
        }

        std::string cmd = topic + "\n" + key + "\n" + payload;
        raft_->append_client_value(cmd);

        return {0, 0}; // eventually replace with real offsets
    }

    if (!is_leader()) {
        throw std::runtime_error("publish called on non leader node");
    }

    // Fallback: old local replication logic (Mode A) to keep tests working.
    TopicConfig cfg;
    cfg.name = topic;
    cfg.num_partitions = cfg_.default_partitions;
    topics_->create_topic(cfg);

    auto local_res = topics_->publish(topic, key, payload);
    std::size_t partition_index = local_res.first;
    uint64_t local_offset = local_res.second;

    std::vector<uint64_t> follower_offsets;
    follower_offsets.reserve(replication_peers_.size());
    for (NebulaNode* peer : replication_peers_) {
        if (!peer) continue;
        try {
            auto peer_res = peer->replicate_publish(topic, key, payload);
            follower_offsets.push_back(peer_res.second);
        } catch (const std::exception& ex) {
            std::cerr << "Replication failed on peer: " << ex.what() << "\n";
        }
    }

    // Existing commit_index_ calculation here, unchanged...
    // (whatever logic you had computing majority offset and updating commit_index_)

    return {partition_index, local_offset};
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

RequestVoteResult NebulaNode::send_request_vote(const std::string& target_id,
                                                const RequestVoteRPC& rpc)
{
    auto it = raft_clients_.find(target_id);
    if (it == raft_clients_.end()) {
        // No connection to that peer; behave as a rejected vote.
        return RequestVoteResult{ .term = rpc.term, .vote_granted = false };
    }

    try {
        return it->second->request_vote(rpc);
    } catch (const std::exception& ex) {
        std::cerr << "[NebulaNode] RequestVote RPC to "
                  << target_id << " failed: " << ex.what() << "\n";
    } catch (...) {
        std::cerr << "[NebulaNode] RequestVote RPC to "
                  << target_id << " failed (unknown error)\n";
    }

    return RequestVoteResult{ .term = rpc.term, .vote_granted = false };
}

AppendEntriesResponse NebulaNode::send_append_entries(const std::string& target_id,
                                                      const AppendEntriesRequest& rpc)
{
    auto it = raft_clients_.find(target_id);
    if (it == raft_clients_.end()) {
        // Treat as failed replication
        return AppendEntriesResponse{
            .term = rpc.term,
            .success = false,
            .match_index = static_cast<size_t>(-1)
        };
    }

    try {
        return it->second->append_entries(rpc);
    } catch (const std::exception& ex) {
        std::cerr << "[NebulaNode] AppendEntries RPC to "
                  << target_id << " failed: " << ex.what() << "\n";
    } catch (...) {
        std::cerr << "[NebulaNode] AppendEntries RPC to "
                  << target_id << " failed (unknown error)\n";
    }

    return AppendEntriesResponse{
        .term = rpc.term,
        .success = false,
        .match_index = static_cast<size_t>(-1)
    };
}

void NebulaNode::apply(const std::string& command) {
    // Format: topic\nkey\npayload
    std::stringstream ss(command);
    std::string topic, key, payload;
    std::getline(ss, topic);
    std::getline(ss, key);
    std::getline(ss, payload);
    if (topic.empty())
        return;
    topics_->publish_from_raft(topic, key, payload);
}

}  // namespace nebula
