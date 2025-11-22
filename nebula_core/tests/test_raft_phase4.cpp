#include "../src/raft.h"
#include <cassert>
#include <iostream>

using nebula::RaftNode;
using nebula::RaftRole;
using nebula::AppendEntriesRequest;
using nebula::LogEntry;

int main() {
    // Build a simple two node cluster
    RaftNode n1("n1");
    RaftNode n2("n2");

    n1.add_peer(&n2);
    n2.add_peer(&n1);

    // Force n1 to become leader
    n1.become_candidate();   // will vote for itself
    n1.become_leader();      // we call directly for the test

    // Append three values from leader
    n1.append_client_value("v0");
    n1.append_client_value("v1");
    n1.append_client_value("v2");

    // After replication commit index on leader should be 2
    assert(n1.commit_index() == 2);

    // Now simulate a new leader with higher term arriving
    AppendEntriesRequest req;
    req.leader_id = "nX";
    req.term = 2; // higher than leader term 1
    req.prev_log_index = n1.log_size() - 1;
    req.prev_log_term = n1.log_at(n1.log_size() - 1).term;
    req.entries.clear();
    req.leader_commit = n1.log_size() - 1;

    auto resp = n1.handle_append_entries(req);
    (void)resp;

    // n1 must step down and accept the new leader
    assert(n1.role() == RaftRole::Follower);
    assert(n1.current_term() == 2);
    assert(n1.leader_id().has_value());
    assert(n1.leader_id().value() == "nX");

    // commit index should still be at least 2
    assert(n1.commit_index() >= 2);

    std::cout << "Phase 4 RAFT leader stepdown and consistency test passed\n";
    return 0;
}
