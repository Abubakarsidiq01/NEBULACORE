#include "../src/raft.h"
#include <cassert>
#include <iostream>

using namespace nebula;

int main() {
    RaftNode n1("n1");
    RaftNode n2("n2");
    RaftNode n3("n3");

    // Fully connected cluster
    n1.add_peer(&n2);
    n1.add_peer(&n3);

    n2.add_peer(&n1);
    n2.add_peer(&n3);

    n3.add_peer(&n1);
    n3.add_peer(&n2);

    // Force n1 through a normal election
    n1.become_candidate(); // will send RequestVote to n2, n3 and become leader

    assert(n1.role() == RaftRole::Leader);
    assert(n1.leader_id().has_value());
    assert(n1.leader_id().value() == "n1");

    // Append three client values
    n1.append_client_value("v1");
    n1.append_client_value("v2");
    n1.append_client_value("v3");

    // Logs must be replicated
    assert(n1.log_size() == 3);
    assert(n2.log_size() == 3);
    assert(n3.log_size() == 3);

    assert(n1.log_at(0).value == "v1");
    assert(n1.log_at(1).value == "v2");
    assert(n1.log_at(2).value == "v3");

    assert(n2.log_at(0).value == "v1");
    assert(n2.log_at(1).value == "v2");
    assert(n2.log_at(2).value == "v3");

    assert(n3.log_at(0).value == "v1");
    assert(n3.log_at(1).value == "v2");
    assert(n3.log_at(2).value == "v3");

    // Commit index should be at the last entry on the leader
    assert(n1.commit_index() == 2);

    std::cout << "Phase 3 RAFT log replication test passed\n";
    return 0;
}
