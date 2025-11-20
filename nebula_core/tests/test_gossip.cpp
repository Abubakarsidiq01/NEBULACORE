#include "../src/cluster.h"

#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>

int main() {
    using namespace nebula;

    boost::asio::io_context io;

    NodeId n1{"node1", "127.0.0.1", 9101};
    NodeId n2{"node2", "127.0.0.1", 9102};
    NodeId n3{"node3", "127.0.0.1", 9103};

    std::vector<NodeId> p1{n2, n3};
    std::vector<NodeId> p2{n1, n3};
    std::vector<NodeId> p3{n1, n2};

    GossipCluster c1(io, n1, p1);
    GossipCluster c2(io, n2, p2);
    GossipCluster c3(io, n3, p3);

    c1.start();
    c2.start();
    c3.start();

    std::thread t([&io]() {
        io.run();
    });

    std::this_thread::sleep_for(std::chrono::seconds(3));

    c1.stop();
    c2.stop();
    c3.stop();
    io.stop();
    t.join();

    auto m1 = c1.members_snapshot();
    auto m2 = c2.members_snapshot();
    auto m3 = c3.members_snapshot();

    auto count_ids = [](const std::vector<MemberView>& v) {
        std::size_t count = 0;
        for (const auto& m : v) {
            if (m.heartbeat > 0) {
                count++;
            }
        }
        return count;
    };

    std::size_t c1_count = count_ids(m1);
    std::size_t c2_count = count_ids(m2);
    std::size_t c3_count = count_ids(m3);

    std::cout << "Cluster1 members: " << c1_count << "\n";
    std::cout << "Cluster2 members: " << c2_count << "\n";
    std::cout << "Cluster3 members: " << c3_count << "\n";

    assert(c1_count >= 2);
    assert(c2_count >= 2);
    assert(c3_count >= 2);

    std::cout << "Phase 4 gossip cluster test passed\n";
    return 0;
}
