#include "../src/raft.h"
#include <cassert>
#include <iostream>

using namespace nebula;

int main() {
    RaftNode n1("n1");
    RaftNode n2("n2");
    RaftNode n3("n3");

    // wire fully connected cluster
    n1.add_peer(&n2); n1.add_peer(&n3);
    n2.add_peer(&n1); n2.add_peer(&n3);
    n3.add_peer(&n1); n3.add_peer(&n2);

    // force n1 to be leader
    n1.become_candidate();
    n1.become_leader();

    // nothing applied yet
    assert(n1.applied_values().empty());
    assert(n2.applied_values().empty());
    assert(n3.applied_values().empty());

    // client sends three values to leader
    n1.append_client_value("a");
    n1.append_client_value("b");
    n1.append_client_value("c");

    // by now, replication + commit logic should have converged
    assert(n1.commit_index() == 2);
    assert(n2.commit_index() == 2);
    assert(n3.commit_index() == 2);

    const auto& a1 = n1.applied_values();
    const auto& a2 = n2.applied_values();
    const auto& a3 = n3.applied_values();

    assert(a1.size() == 3);
    assert(a2.size() == 3);
    assert(a3.size() == 3);

    assert(a1[0] == "a" && a1[1] == "b" && a1[2] == "c");
    assert(a2[0] == "a" && a2[1] == "b" && a2[2] == "c");
    assert(a3[0] == "a" && a3[1] == "b" && a3[2] == "c");

    std::cout << "Phase 6 RAFT state machine application test passed" << std::endl;
    return 0;
}
