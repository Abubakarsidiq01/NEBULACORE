#include "../src/raft.h"
#include <iostream>
#include <thread>
#include <chrono>

int main() {
    using namespace nebula;

    RaftNode n("node1");

    for (int i = 0; i < 600; i++) {
        n.tick();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    std::cout << "Phase 1 RAFT basic timer test passed\n";
    return 0;
}
