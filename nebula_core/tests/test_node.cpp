#include "../src/nebula_node.h"

#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>

int main() {
    using namespace nebula;

    std::filesystem::remove_all("node1_data");
    std::filesystem::remove_all("node2_data");
    std::filesystem::remove_all("node3_data");

    NodeId n1{"node1", "127.0.0.1", 9301};
    NodeId n2{"node2", "127.0.0.1", 9302};
    NodeId n3{"node3", "127.0.0.1", 9303};

    std::vector<NodeId> all{n1, n2, n3};

    NebulaNodeConfig c1;
    c1.data_root = "node1_data";
    c1.self = n1;
    c1.peers = {n2, n3};
    c1.default_partitions = 3;

    NebulaNodeConfig c2;
    c2.data_root = "node2_data";
    c2.self = n2;
    c2.peers = {n1, n3};
    c2.default_partitions = 3;

    NebulaNodeConfig c3;
    c3.data_root = "node3_data";
    c3.self = n3;
    c3.peers = {n1, n2};
    c3.default_partitions = 3;

    NebulaNode node1(c1);
    NebulaNode node2(c2);
    NebulaNode node3(c3);

    node1.start();
    node2.start();
    node3.start();

    // Let gossip and leader election converge
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
        assert(false && "Unknown leader id");
    }

    std::cout << "Cluster leader resolved to: " << leader_id << "\n";

    auto [pidx, off] = leader->publish("alerts", "deviceA", "hello");
    auto msg = leader->consume("alerts", "cg1", pidx);
    auto msg2 = leader->consume("alerts", "cg1", pidx);

    assert(msg.has_value());
    assert(*msg == "hello");
    assert(!msg2.has_value());

    std::cout << "NebulaNode integrated test passed\n";

    node1.stop();
    node2.stop();
    node3.stop();

    return 0;
}
