#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>

#include "nebula_node.h"
#include "raft_server.h"

using namespace nebula;

// Simple parser for peer specs of the form "id:host:port"
static bool parse_peer_spec(const std::string& spec, NodeId& out) {
    std::string id;
    std::string host;
    std::string port_str;

    std::size_t first_colon = spec.find(':');
    if (first_colon == std::string::npos) {
        return false;
    }
    std::size_t second_colon = spec.find(':', first_colon + 1);
    if (second_colon == std::string::npos) {
        return false;
    }

    id = spec.substr(0, first_colon);
    host = spec.substr(first_colon + 1, second_colon - first_colon - 1);
    port_str = spec.substr(second_colon + 1);

    if (id.empty() || host.empty() || port_str.empty()) {
        return false;
    }

    uint16_t gossip_port = 0;
    try {
        int p = std::stoi(port_str);
        if (p < 0 || p > 65535) {
            return false;
        }
        gossip_port = static_cast<uint16_t>(p);
    } catch (...) {
        return false;
    }

    out.id = id;
    out.host = host;
    out.gossip_port = gossip_port;
    return true;
}

int main(int argc, char** argv) {
    // Expected usage:
    // nebula_node_main <id> <host> <gossip_port> [peer_id:peer_host:peer_gossip_port ...]
    //
    // Example for three terminals:
    //
    // Terminal 1:
    //   nebula_node_main n1 127.0.0.1 5001 n2:127.0.0.1:5002 n3:127.0.0.1:5003
    //
    // Terminal 2:
    //   nebula_node_main n2 127.0.0.1 5002 n1:127.0.0.1:5001 n3:127.0.0.1:5003
    //
    // Terminal 3:
    //   nebula_node_main n3 127.0.0.1 5003 n1:127.0.0.1:5001 n2:127.0.0.1:5002

    if (argc < 4) {
        std::cerr << "Usage: " << argv[0]
                  << " <id> <host> <gossip_port> [peer_id:peer_host:peer_gossip_port ...]\n";
        return 1;
    }

    std::string self_id = argv[1];
    std::string self_host = argv[2];
    uint16_t self_gossip_port = 0;

    try {
        int p = std::stoi(argv[3]);
        if (p < 0 || p > 65535) {
            std::cerr << "Invalid gossip port\n";
            return 1;
        }
        self_gossip_port = static_cast<uint16_t>(p);
    } catch (...) {
        std::cerr << "Invalid gossip port\n";
        return 1;
    }

    NodeId self{};
    self.id = self_id;
    self.host = self_host;
    self.gossip_port = self_gossip_port;

    std::vector<NodeId> peers;
    for (int i = 4; i < argc; ++i) {
        NodeId peer{};
        if (!parse_peer_spec(argv[i], peer)) {
            std::cerr << "Invalid peer spec: " << argv[i]
                      << " expected id:host:gossip_port\n";
            return 1;
        }
        // ignore self if accidentally listed
        if (peer.id == self.id) {
            continue;
        }
        peers.push_back(peer);
    }

    // Data directory per node so logs do not clash
    std::string data_root = "data_" + self.id;

    NebulaNodeConfig cfg;
    cfg.data_root = data_root;
    cfg.self = self;
    cfg.peers = peers;
    cfg.default_partitions = 3;

    // Build NebulaNode
    NebulaNode node(cfg);

    // Wire RaftNode for this process
    node.raft_ = std::make_unique<RaftNode>(cfg.self.id);
    node.raft_->set_state_machine(&node);


    // Mode B: set peer IDs based on config
    {
        std::vector<std::string> peer_ids;
        peer_ids.reserve(cfg.peers.size());
        for (const auto& p : cfg.peers) {
            if (p.id != cfg.self.id) {
                peer_ids.push_back(p.id);
            }
        }
        node.raft_->set_peer_ids(peer_ids);

        // Wire NebulaNode as the IRaftTransport
        node.raft_->set_transport(&node);
    }

    // RaftServer listens on host and a port derived from gossip port
    RaftServerConfig rsc;
    rsc.host = cfg.self.host;
    rsc.port = static_cast<uint16_t>(cfg.self.gossip_port + 1000);

    node.raft_server_ = std::make_unique<RaftServer>(rsc, *node.raft_);

    // Start RaftServer first so peers can connect
    std::cout << "[NebulaNode " << cfg.self.id
              << "] starting RaftServer on "
              << rsc.host << ":" << rsc.port << "\n";
    node.raft_server_->start();

    // Now start NebulaNode gossip cluster and RaftClients
    std::cout << "[NebulaNode " << cfg.self.id << "] starting NebulaNode core\n";
    node.start();

    if (cfg.self.id == "n1") {
        std::thread([&]() {
            std::this_thread::sleep_for(std::chrono::seconds(3));
            std::cout << "\n[PUBLISH TEST] Publishing test message...\n";
            try {
                node.publish("alerts", "k1", "hello world from raft");
            } catch (const std::exception& ex) {
                std::cerr << "[PUBLISH ERROR] " << ex.what() << "\n";
            }
        }).detach();
    }
    

    if (!node.raft_) {
        std::cerr << "Raft node was not created, exiting\n";
        return 1;
    }

    // Raft tick loop
    std::atomic<bool> running{true};
    std::thread raft_thread([&]() {
        while (running.load()) {
            node.raft_->tick();
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    });

    std::cout << "[NebulaNode " << cfg.self.id
              << "] running. Press Ctrl+C to terminate process.\n";

    // Block forever. Normal exit is via process kill or Ctrl+C.
    for (;;) {
        std::this_thread::sleep_for(std::chrono::seconds(60));
    }

    // Unreachable with current design, kept for completeness
    running.store(false);
    if (raft_thread.joinable()) {
        raft_thread.join();
    }

    node.stop();
    if (node.raft_server_) {
        node.raft_server_->stop();
    }

    return 0;
}
