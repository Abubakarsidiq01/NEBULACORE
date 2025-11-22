#include "../src/raft.h"
#include <thread>
#include <chrono>
#include <iostream>
#include <cassert>  // <-- needed for assert

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

    // Drive the timers
    for (int i = 0; i < 800; i++) {
        n1.tick();
        n2.tick();
        n3.tick();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    int leaders = 0;
    if (n1.role() == RaftRole::Leader) leaders++;
    if (n2.role() == RaftRole::Leader) leaders++;
    if (n3.role() == RaftRole::Leader) leaders++;

    std::cout << "Leaders elected: " << leaders << "\n";
    assert(leaders == 1);

    std::cout << "Phase 2 RAFT RequestVote test passed\n";
    return 0;
}
