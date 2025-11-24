#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <optional>

#include <boost/asio.hpp>
#include <unordered_map>
#include <functional>

#include "cluster.h"
#include "leader.h"
#include "topic_manager.h"
#include "raft.h"
#include "raft_server.h"

namespace nebula {

struct NebulaNodeConfig {
    std::string data_root;
    NodeId self;
    std::vector<NodeId> peers;
    std::size_t default_partitions{3};
};

class NebulaNode : public IRaftStateMachine, public IRaftTransport {
public:
    explicit NebulaNode(const NebulaNodeConfig& cfg);

    void start();
    void stop();

    bool is_leader() const;
    std::optional<std::string> leader_id() const;

    // Leader-only publish (performs replication)
    std::pair<std::size_t, uint64_t> publish(
        const std::string& topic,
        const std::string& key,
        const std::string& payload);

    // Follower append path (no leader checks)
    std::pair<std::size_t, uint64_t> replicate_publish(
        const std::string& topic,
        const std::string& key,
        const std::string& payload);

    // Consuming messages from local partitions
    std::optional<std::string> consume(
        const std::string& topic,
        const std::string& consumer_group,
        std::size_t partition_index);

    // Replication peers (other NebulaNode instances)
    void set_replication_peers(const std::vector<NebulaNode*>& peers);

    // Return the committed offset for a topic partition on this node
    // Zero means nothing committed yet
    uint64_t committed_offset(const std::string& topic, std::size_t partition) const;

    const NebulaNodeConfig& config() const { return cfg_; }

    // --------------------------------------------------------------------
    // *** Public Raft integration so tests & networking setup can access ***
    // --------------------------------------------------------------------
    std::unique_ptr<RaftNode> raft_;          // now PUBLIC
    std::unique_ptr<RaftServer> raft_server_; // now PUBLIC

    // --------------------------------------------
    // IRaftTransport implementation
    // --------------------------------------------
    RequestVoteResult send_request_vote(const std::string& target_id,
        const RequestVoteRPC& rpc) override;

    AppendEntriesResponse send_append_entries(const std::string& target_id,
                const AppendEntriesRequest& rpc) override;

    // IRaftStateMachine implementation: apply a committed Raft command.
    void apply(const std::string& command) override;


private:
    struct PartitionKey {
        std::string topic;
        std::size_t partition;

        bool operator==(const PartitionKey& other) const {
            return partition == other.partition && topic == other.topic;
        }
    };

    struct PartitionKeyHash {
        std::size_t operator()(const PartitionKey& k) const noexcept {
            std::size_t h1 = std::hash<std::string>{}(k.topic);
            std::size_t h2 = std::hash<std::size_t>{}(k.partition);
            return h1 ^ (h2 + 0x9e3779b97f4a7c15ULL + (h1 << 6) + (h1 >> 2));
        }
    };

    NebulaNodeConfig cfg_;

    boost::asio::io_context io_;

    using WorkGuard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;
    std::optional<WorkGuard> work_guard_;

    std::thread io_thread_;

    std::unique_ptr<GossipCluster> cluster_;
    std::unique_ptr<LeaderElector> elector_;
    std::unique_ptr<TopicManager> topics_;

    std::vector<NebulaNode*> replication_peers_;

    // Per partition commit index, only meaningful on leader
    std::unordered_map<PartitionKey, uint64_t, PartitionKeyHash> commit_index_;

    std::unordered_map<std::string, std::unique_ptr<RaftClient>> raft_clients_;


    void run_io();
};

}  // namespace nebula
