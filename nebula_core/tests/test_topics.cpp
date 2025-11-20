#include "../src/topic_manager.h"

#include <cassert>
#include <filesystem>
#include <iostream>

int main() {
    using namespace nebula;

    std::string root = "test_topics_data";
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);

    TopicManager tm(root);

    TopicConfig cfg{"alerts", 3};
    tm.create_topic(cfg);

    // Use keys that are more likely to map to different partitions
    auto [p0_a, off0_a] = tm.publish("alerts", "key0", "A1");
    auto [p1_b, off0_b] = tm.publish("alerts", "key1", "B1");
    auto [p0_a_2, off1_a] = tm.publish("alerts", "key0", "A2");

    // Ensure A messages go to the same partition
    assert(p0_a == p0_a_2);

    // Ensure B goes to a different partition
    assert(p0_a != p1_b);

    std::string group = "g1";

    auto msg1 = tm.read_next("alerts", group, p0_a);
    auto msg2 = tm.read_next("alerts", group, p0_a);
    auto msg3 = tm.read_next("alerts", group, p0_a);

    assert(msg1.has_value() && *msg1 == "A1");
    assert(msg2.has_value() && *msg2 == "A2");
    assert(!msg3.has_value());

    auto off = tm.current_offset("alerts", group, p0_a);
    assert(off.has_value());
    assert(*off == 2);

    // Read B1
    auto msg_b1 = tm.read_next("alerts", group, p1_b);
    auto msg_b2 = tm.read_next("alerts", group, p1_b);

    assert(msg_b1.has_value() && *msg_b1 == "B1");
    assert(!msg_b2.has_value());

    std::cout << "Phase 3 topic manager test passed\n";
    return 0;
}
