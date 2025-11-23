#include "../src/raft.h"
#include <cassert>
#include <iostream>

using namespace nebula;

int main() {
    std::cout << "Running Phase 8 RAFT snapshot test...\n";

    RaftNode n1("node1");
    RaftNode n2("node2");
    RaftNode n3("node3");

    n1.add_peer(&n2);
    n1.add_peer(&n3);
    n2.add_peer(&n1);
    n2.add_peer(&n3);
    n3.add_peer(&n1);
    n3.add_peer(&n2);

    // ----- ELECT LEADER -----
    n1.tick(); n2.tick(); n3.tick();
    n1.tick(); n2.tick(); n3.tick();
    assert(n1.role() == RaftRole::Leader);

    // Append entries: "A", "B", "C", "D", "E"
    n1.append_client_value("A");
    n1.append_client_value("B");
    n1.append_client_value("C");
    n1.append_client_value("D");
    n1.append_client_value("E");

    // All nodes must commit at least 4 entries by now
    assert(n2.commit_index() == 4);
    assert(n3.commit_index() == 4);

    // ----- TAKE SNAPSHOT ON LEADER -----
    n1.take_snapshot();

    // Snapshot must contain A,B,C,D,E
    const auto& state = n1.snapshot_state();
    assert(state.size() == 5);
    assert(state[0] == "A");
    assert(state[4] == "E");

    // Snapshot metadata
    assert(n1.snapshot_index() == 4);
    assert(n1.snapshot_term() == n1.current_term());

    // After snapshot, log_size() must report zero
    assert(n1.log_size() == 0);

    // ----- INSTALL SNAPSHOT ON FOLLOWERS -----
    n2.install_snapshot(n1.snapshot_index(), n1.snapshot_term(), n1.snapshot_state());
    n3.install_snapshot(n1.snapshot_index(), n1.snapshot_term(), n1.snapshot_state());

    // Followers now reflect snapshot state
    assert(n2.snapshot_index() == 4);
    assert(n3.snapshot_index() == 4);
    assert(n2.snapshot_state().size() == 5);
    assert(n3.snapshot_state().size() == 5);

    // Followers also must report empty log
    assert(n2.log_size() == 0);
    assert(n3.log_size() == 0);

    std::cout << "Phase 8 RAFT snapshot test passed\n";
    return 0;
}