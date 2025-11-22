#include "../src/raft.h"
#include <cassert>
#include <iostream>

using namespace nebula;

int main() {
    RaftNode n1("n1");
    RaftNode n2("n2");
    RaftNode n3("n3");

    n1.add_peer(&n2);
    n1.add_peer(&n3);
    n2.add_peer(&n1);
    n2.add_peer(&n3);
    n3.add_peer(&n1);
    n3.add_peer(&n2);

    // Elect initial leader
    n1.become_candidate(); 
    assert(n1.role() == RaftRole::Leader);

    // Append 3 values
    n1.append_client_value("A");
    n1.append_client_value("B");
    n1.append_client_value("C");

    // All must commit index 2 (0,1,2)
    assert(n1.commit_index() == 2);
    assert(n2.commit_index() == 2);
    assert(n3.commit_index() == 2);

    // Force new election with higher term
    n2.become_candidate();
    assert(n2.role() == RaftRole::Leader);

    // After leadership transfer, commit index must remain safe
    assert(n2.commit_index() == 2);
    assert(n1.commit_index() == 2);
    assert(n3.commit_index() == 2);

    std::cout << "Phase 5 RAFT stability test passed" << std::endl;
    return 0;
}
