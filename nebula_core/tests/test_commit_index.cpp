#include "../src/nebula_node.h"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>

int main() {
    using namespace nebula;

    // Clean old data
    std::filesystem::remove_all("ci_node1_data");
    std::filesystem::remove_all("ci_node2_data");
    std::filesystem::remove_all("ci_node3_data");

    NodeId n1{"node1", "127.0.0.1", 9501};
    NodeId n2{"node2", "127.0.0.1", 9502};
    NodeId n3{"node3", "127.0.0.1", 9503};

    NebulaNodeConfig c1;
    c1.data_root = "ci_node1_data";
    c1.self = n1;
    c1.peers = {n2, n3};
    c1.default_partitions = 3;

    NebulaNodeConfig c2;
    c2.data_root = "ci_node2_data";
    c2.self = n2;
    c2.peers = {n1, n3};
    c2.default_partitions = 3;

    NebulaNodeConfig c3;
    c3.data_root = "ci_node3_data";
    c3.self = n3;
    c3.peers = {n1, n2};
    c3.default_partitions = 3;

    NebulaNode node1(c1);
    NebulaNode node2(c2);
    NebulaNode node3(c3);

    // Set replication peers just like test_replication
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

    std::cout << "Cluster leader is: " << leader_id << "\n";

    std::string topic = "orders";
    std::string key = "k";

    auto [p0, o1] = leader->publish(topic, key, "m1");
    auto [p1, o2] = leader->publish(topic, key, "m2");
    auto [p2, o3] = leader->publish(topic, key, "m3");

    // All should go to same partition if key is same
    assert(p0 == p1);
    assert(p1 == p2);

    uint64_t committed = leader->committed_offset(topic, p0);

    std::cout << "Offsets: " << o1 << ", " << o2 << ", " << o3 << "\n";
    std::cout << "Committed offset on leader partition " << p0
              << " is " << committed << "\n";

    // With three nodes and synchronous replication, commit index
    // should at least reach the second message
    assert(committed >= o2);

    node1.stop();
    node2.stop();
    node3.stop();

    std::cout << "Commit index test passed\n";
    return 0;
}
