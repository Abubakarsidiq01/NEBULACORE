#include "../src/nebula_log.h"
#include <cassert>
#include <iostream>
#include <filesystem>

int main() {
    // Clean test dir
    std::string dir = "test_logs";
    std::filesystem::create_directories(dir);
    std::string file = "segment_00000000.log";
    std::filesystem::remove(dir + "/" + file);

    nebula::LogConfig cfg{dir, file};
    nebula::NebulaLog log(cfg);

    // Append records
    uint64_t off0 = log.append("hello");
    uint64_t off1 = log.append("world");
    uint64_t off2 = log.append("nebula");

    assert(off0 == 0);
    assert(off1 == 1);
    assert(off2 == 2);

    auto v0 = log.read(0);
    auto v1 = log.read(1);
    auto v2 = log.read(2);
    auto v3 = log.read(3);

    assert(v0.has_value() && *v0 == "hello");
    assert(v1.has_value() && *v1 == "world");
    assert(v2.has_value() && *v2 == "nebula");
    assert(!v3.has_value());

    // Test iterator
    auto it = log.scan(0);
    int count = 0;
    while (it.next()) {
        std::cout << it.offset() << ": " << it.value() << "\n";
        count++;
    }
    assert(count == 3);

    std::cout << "All Phase 1 tests passed\n";
    return 0;
}
