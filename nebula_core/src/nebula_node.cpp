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
        raft_->set_peer_ids(peer_ids);
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

// IMPORTANT: when Raft is wired, this is the Raft leader, not gossip.
bool NebulaNode::is_leader() const {
    if (raft_) {
        return raft_->role() == RaftRole::Leader;
    }
    return elector_->is_leader();
}

// IMPORTANT: when Raft is wired, leader_id comes from Raft.
std::optional<std::string> NebulaNode::leader_id() const {
    if (raft_) {
        auto lid = raft_->leader_id();
        if (lid.has_value()) {
            return lid;
        }
        // fall through to gossip if Raft doesn't know yet
    }
    return elector_->leader_id();
}

// Return (host, client_port) for the Raft leader (client_port = gossip_port + 2000)
std::pair<std::string, uint16_t> NebulaNode::leader_address() const {
    auto lid_opt = leader_id();
    if (!lid_opt.has_value()) {
        return {"", 0};
    }
    const std::string& lid = *lid_opt;

    // Self is leader
    if (lid == cfg_.self.id) {
        return {cfg_.self.host,
                static_cast<uint16_t>(cfg_.self.gossip_port + 2000)};
    }

    // Find in peer list
    for (const auto& p : cfg_.peers) {
        if (p.id == lid) {
            return {p.host,
                    static_cast<uint16_t>(p.gossip_port + 2000)};
        }
    }

    return {"", 0}; // unknown leader
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
    - Append locally via Raft
    - Replicate to followers
    - Wait for commit + apply()
*/
std::pair<std::size_t, uint64_t> NebulaNode::publish(
    const std::string& topic,
    const std::string& key,
    const std::string& payload)
{
    // If Raft is wired, use Raft-backed pipeline
    if (raft_) {
        if (raft_->role() != RaftRole::Leader) {
            throw std::runtime_error("publish called on follower");
        }

        // 1. Allocate publish ID
        uint64_t pub_id;
        {
            std::lock_guard<std::mutex> lock(publish_mutex_);
            pub_id = next_publish_id_++;
            waiting_publish_[pub_id] = PublishWait{};
        }

        // 2. Encode command: pub_id\ntopic\nkey\npayload
        std::string cmd = std::to_string(pub_id);
        cmd.push_back('\n');
        cmd += topic;
        cmd.push_back('\n');
        cmd += key;
        cmd.push_back('\n');
        cmd += payload;

        // 3. Prepare future to wait on
        auto& pw = waiting_publish_.at(pub_id);
        std::future<std::pair<std::size_t, uint64_t>> fut =
            pw.promise.get_future();

        // 4. Append to Raft log
        raft_->append_client_value(cmd);

        // 5. Wait for apply() to fulfill the promise
        auto result = fut.get(); // {partition, offset}

        // 6. Cleanup
        {
            std::lock_guard<std::mutex> lock(publish_mutex_);
            waiting_publish_.erase(pub_id);
        }

        return result;
    }

    // ---------------------------
    // LEGACY MODE (NO RAFT)
    // ---------------------------
    if (!is_leader()) {
        throw std::runtime_error("publish called on non-leader (legacy mode)");
    }

    TopicConfig cfg;
    cfg.name = topic;
    cfg.num_partitions = cfg_.default_partitions;
    topics_->create_topic(cfg);

    auto local_res = topics_->publish(topic, key, payload);

    // Legacy replication for in-process test mode
    for (NebulaNode* peer : replication_peers_) {
        if (!peer) continue;
        try {
            peer->replicate_publish(topic, key, payload);
        } catch (...) {}
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

// Called by Raft state machine to apply committed commands
void NebulaNode::apply(const std::string& command)
{
    // Decode "pub_id\ntopic\nkey\npayload"
    std::stringstream ss(command);

    std::string pub_id_str;
    std::string topic;
    std::string key;
    std::string payload;

    std::getline(ss, pub_id_str);
    std::getline(ss, topic);
    std::getline(ss, key);
    std::getline(ss, payload);

    if (topic.empty()) {
        std::cerr << "[NebulaNode " << cfg_.self.id
                  << "] apply: empty topic\n";
        return;
    }

    uint64_t pub_id = 0;
    try { pub_id = std::stoull(pub_id_str); }
    catch (...) { pub_id = 0; }

    // Ensure topic exists
    TopicConfig cfg;
    cfg.name = topic;
    cfg.num_partitions = cfg_.default_partitions;
    topics_->create_topic(cfg);

    // Write to partitioned WAL
    auto [partition, offset] =
        topics_->publish(topic, key, payload);

    // Update committed offset index for this topic partition
    {
        PartitionKey pk{topic, partition};
        commit_index_[pk] = offset;
    }

    // Only the Raft leader fulfills publish wait.
    if (raft_ && raft_->role() == RaftRole::Leader && pub_id != 0) {
        std::lock_guard<std::mutex> lock(publish_mutex_);
        auto it = waiting_publish_.find(pub_id);
        if (it != waiting_publish_.end()) {
            it->second.promise.set_value(
                std::pair<std::size_t,uint64_t>(partition, offset)
            );
        }
    }
}

}  // namespace nebula
