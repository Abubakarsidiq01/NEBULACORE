#include "nebula_node.h"
#include "raft_server.h"
#include "raft.h"

#include <thread>
#include <chrono>
#include <iostream>

using namespace nebula;

// Helper to build a NodeId
static NodeId make_node_id(const std::string& id, uint16_t gossip_port) {
    NodeId n;
    n.id = id;
    n.host = "127.0.0.1";
    n.gossip_port = gossip_port;
    return n;
}

// Create NebulaNode + RaftNode + RaftServer
static NebulaNode* make_node(const std::string& id,
                             uint16_t raft_port,
                             uint16_t gossip_port,
                             const std::vector<NodeId>& all_nodes)
{
    NebulaNodeConfig cfg{
        .data_root = "./data_" + id,
        .self = make_node_id(id, gossip_port),
        .peers = all_nodes,
        .default_partitions = 1
    };

    auto* node = new NebulaNode(cfg);

    // Create Raft instance
    node->raft_ = std::make_unique<RaftNode>(id);

    // Create RaftServer
    RaftServerConfig rsc{
        .host = "127.0.0.1",
        .port = raft_port
    };
    node->raft_server_ = std::make_unique<RaftServer>(rsc, *node->raft_);

    return node;
}

int main() {
    // Gossip ports (one per node)
    uint16_t g1 = 5001;
    uint16_t g2 = 5002;
    uint16_t g3 = 5003;

    // Raft ports (one per node)
    uint16_t r1 = 6001;
    uint16_t r2 = 6002;
    uint16_t r3 = 6003;

    // Build NodeId list for cluster
    std::vector<NodeId> all_nodes = {
        make_node_id("n1", g1),
        make_node_id("n2", g2),
        make_node_id("n3", g3)
    };

    // Construct three NebulaNodes
    NebulaNode* n1 = make_node("n1", r1, g1, all_nodes);
    NebulaNode* n2 = make_node("n2", r2, g2, all_nodes);
    NebulaNode* n3 = make_node("n3", r3, g3, all_nodes);

    // Start RaftServers (network listeners)
    n1->raft_server_->start();
    n2->raft_server_->start();
    n3->raft_server_->start();

    // Small delay to let servers start
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Manual Raft ticks (no networking hooked yet)
    for (int i = 0; i < 2000; i++) {
        n1->raft_->tick();
        n2->raft_->tick();
        n3->raft_->tick();
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    auto r1_role = n1->raft_->role();
    auto r2_role = n2->raft_->role();
    auto r3_role = n3->raft_->role();

    std::cout << "n1 role=" << (int)r1_role << "\n";
    std::cout << "n2 role=" << (int)r2_role << "\n";
    std::cout << "n3 role=" << (int)r3_role << "\n";

    if (r1_role == RaftRole::Leader ||
        r2_role == RaftRole::Leader ||
        r3_role == RaftRole::Leader) {
        std::cout << "PASS: a leader was elected\n";
    } else {
        std::cerr << "FAIL: no leader elected\n";
        return 1;
    }

    // Clean shutdown
    n1->raft_server_->stop();
    n2->raft_server_->stop();
    n3->raft_server_->stop();

    delete n1;
    delete n2;
    delete n3;

    return 0;
}
