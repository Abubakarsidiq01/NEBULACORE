#include "../src/cluster.h"
#include "../src/leader.h"

#include <cassert>
#include <chrono>
#include <iostream>
#include <thread>

int main() {
    using namespace nebula;

    boost::asio::io_context io;

    NodeId n1{"node1", "127.0.0.1", 9201};
    NodeId n2{"node2", "127.0.0.1", 9202};
    NodeId n3{"node3", "127.0.0.1", 9203};

    std::vector<NodeId> p1{n2, n3};
    std::vector<NodeId> p2{n1, n3};
    std::vector<NodeId> p3{n1, n2};

    GossipCluster c1(io, n1, p1);
    GossipCluster c2(io, n2, p2);
    GossipCluster c3(io, n3, p3);

    LeaderElector e1(io, c1, n1.id);
    LeaderElector e2(io, c2, n2.id);
    LeaderElector e3(io, c3, n3.id);

    c1.start();
    c2.start();
    c3.start();

    e1.start();
    e2.start();
    e3.start();

    std::thread t([&io]() {
        io.run();
    });

    // let gossip + leader ticks converge
    std::this_thread::sleep_for(std::chrono::seconds(3));

    auto l1 = e1.leader_id();
    auto l2 = e2.leader_id();
    auto l3 = e3.leader_id();

    std::cout << "Initial leaders: "
              << (l1.has_value() ? *l1 : "none") << ", "
              << (l2.has_value() ? *l2 : "none") << ", "
              << (l3.has_value() ? *l3 : "none") << "\n";

    assert(l1.has_value());
    assert(l2.has_value());
    assert(l3.has_value());

    // all three must agree on the same leader
    assert(*l1 == *l2);
    assert(*l2 == *l3);

    std::string first_leader = *l1;
    std::cout << "First elected leader: " << first_leader << "\n";

    // Now simulate leader failure by stopping node1 if it's leader,
    // otherwise stop whichever node was chosen.
    if (first_leader == n1.id) {
        c1.stop();
    } else if (first_leader == n2.id) {
        c2.stop();
    } else if (first_leader == n3.id) {
        c3.stop();
    }

    // give time for failure timeout (we set 5s in cluster)
    std::this_thread::sleep_for(std::chrono::seconds(6));

    auto l1b = e1.leader_id();
    auto l2b = e2.leader_id();
    auto l3b = e3.leader_id();

    std::cout << "Leaders after simulated failure: "
              << (l1b.has_value() ? *l1b : "none") << ", "
              << (l2b.has_value() ? *l2b : "none") << ", "
              << (l3b.has_value() ? *l3b : "none") << "\n";

    // some nodes might not have an updated view if they were stopped,
    // but any node with a leader must have a NEW leader different from the first
    if (l1b.has_value()) {
        assert(*l1b != first_leader);
    }
    if (l2b.has_value()) {
        assert(*l2b != first_leader);
    }
    if (l3b.has_value()) {
        assert(*l3b != first_leader);
    }

    // cleanup
    c1.stop();
    c2.stop();
    c3.stop();
    e1.stop();
    e2.stop();
    e3.stop();
    io.stop();
    t.join();

    std::cout << "Phase 5 leader election test passed\n";
    return 0;
}
