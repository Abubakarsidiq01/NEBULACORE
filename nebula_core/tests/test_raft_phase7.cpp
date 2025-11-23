#include "../src/raft.h"

#include <cassert>
#include <iostream>

using namespace nebula;

int main() {
    // Three–node cluster
    RaftNode n1("n1");
    RaftNode n2("n2");
    RaftNode n3("n3");

    n1.add_peer(&n2);
    n1.add_peer(&n3);

    n2.add_peer(&n1);
    n2.add_peer(&n3);

    n3.add_peer(&n1);
    n3.add_peer(&n2);

    // Elect n1 as leader
    n1.become_candidate();
    // Our Raft implementation immediately sends RequestVote and may become leader
    assert(n1.role() == RaftRole::Leader);
    std::cout << "n1 is LEADER for term " << n1.current_term() << "\n";

    // Append three client values on the leader
    n1.append_client_value("v0");
    n1.append_client_value("v1");
    n1.append_client_value("v2");

    // After append + replication, commit_index should reach 2 on the leader
    assert(n1.commit_index() == 2);

    // Now force another election somewhere else to stress stability.
    // We simulate a “view change” style churn but expect the *committed*
    // prefix (up to index 2) to remain consistent on *all* nodes.

    // Let n2 eventually become leader by ticking everyone for a while.
    for (int i = 0; i < 20; ++i) {
        n1.tick();
        n2.tick();
        n3.tick();
    }

    // One of the nodes should be leader; we just care about safety.
    int leaders = 0;
    if (n1.role() == RaftRole::Leader) leaders++;
    if (n2.role() == RaftRole::Leader) leaders++;
    if (n3.role() == RaftRole::Leader) leaders++;

    assert(leaders == 1);

    // Regardless of who is leader now, the committed prefix [0,2]
    // must be present and identical on every node.

    // All nodes should have at least 3 log entries
    assert(n1.log_size() >= 3);
    assert(n2.log_size() >= 3);
    assert(n3.log_size() >= 3);

    // Values must match across nodes for indices 0,1,2
    const char* expected0 = n1.log_at(0).value.c_str();
    const char* expected1 = n1.log_at(1).value.c_str();
    const char* expected2 = n1.log_at(2).value.c_str();

    assert(n2.log_at(0).value == expected0);
    assert(n2.log_at(1).value == expected1);
    assert(n2.log_at(2).value == expected2);

    assert(n3.log_at(0).value == expected0);
    assert(n3.log_at(1).value == expected1);
    assert(n3.log_at(2).value == expected2);

    std::cout << "Phase 7 RAFT churn / stability test passed\n";
    return 0;
}
