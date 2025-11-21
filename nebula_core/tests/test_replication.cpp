#include "../src/nebula_node.h"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>

int main() {
    using namespace nebula;

    // Clean old data
    std::filesystem::remove_all("r_node1_data");
    std::filesystem::remove_all("r_node2_data");
    std::filesystem::remove_all("r_node3_data");

    NodeId n1{"node1", "127.0.0.1", 9401};
    NodeId n2{"node2", "127.0.0.1", 9402};
    NodeId n3{"node3", "127.0.0.1", 9403};

    NebulaNodeConfig c1;
    c1.data_root = "r_node1_data";
    c1.self = n1;
    c1.peers = {n2, n3};
    c1.default_partitions = 3;

    NebulaNodeConfig c2;
    c2.data_root = "r_node2_data";
    c2.self = n2;
    c2.peers = {n1, n3};
    c2.default_partitions = 3;

    NebulaNodeConfig c3;
    c3.data_root = "r_node3_data";
    c3.self = n3;
    c3.peers = {n1, n2};
    c3.default_partitions = 3;

    NebulaNode node1(c1);
    NebulaNode node2(c2);
    NebulaNode node3(c3);

    // Set replication peers (all-to-all for now, only leader matters)
    node1.set_replication_peers({&node2, &node3});
    node2.set_replication_peers({&node1, &node3});
    node3.set_replication_peers({&node1, &node2});

    node1.start();
    node2.start();
    node3.start();

    // Let gossip + leader election converge
    std::this_thread::sleep_for(std::chrono::seconds(4));

    auto l1 = node1.leader_id();
    auto l2 = node2.leader_id();
    auto l3 = node3.leader_id();

    std::cout << "Leaders from nodes: "
              << (l1 ? *l1 : "none") << ", "
              << (l2 ? *l2 : "none") << ", "
              << (l3 ? *l3 : "none") << "\n";

    assert(l1.has_value());
    assert(l2.has_value());
    assert(l3.has_value());
    assert(*l1 == *l2);
    assert(*l2 == *l3);

    std::string leader_id = *l1;

    NebulaNode* leader = nullptr;
    if (leader_id == c1.self.id) {
        leader = &node1;
    } else if (leader_id == c2.self.id) {
        leader = &node2;
    } else if (leader_id == c3.self.id) {
        leader = &node3;
    } else {
        assert(false && "unknown leader id");
    }

    std::cout << "Cluster leader (for replication) is: " << leader_id << "\n";

    // Publish a couple of records through the leader
    std::string topic = "repl_alerts";
    std::string key = "device42";

    auto [pidx, off1] = leader->publish(topic, key, "msg1");
    auto [pidx2, off2] = leader->publish(topic, key, "msg2");

    // Both messages must go to the same partition
    assert(pidx == pidx2);

    // Helper to read two messages from a node and verify they match
    auto check_node = [&](NebulaNode& node, const std::string& label) {
        auto m1 = node.consume(topic, "cg_repl", pidx);
        auto m2 = node.consume(topic, "cg_repl", pidx);
        auto m3 = node.consume(topic, "cg_repl", pidx);

        std::cout << label << " m1="
                  << (m1 ? *m1 : std::string("none"))
                  << " m2="
                  << (m2 ? *m2 : std::string("none"))
                  << " m3="
                  << (m3 ? *m3 : std::string("none"))
                  << "\n";

        assert(m1.has_value() && *m1 == "msg1");
        assert(m2.has_value() && *m2 == "msg2");
        assert(!m3.has_value()); // only two records
    };

    check_node(node1, "node1");
    check_node(node2, "node2");
    check_node(node3, "node3");

    node1.stop();
    node2.stop();
    node3.stop();

    std::cout << "Replication test passed\n";
    return 0;
}
