#include "../src/nebula_log.h"

#include <cassert>
#include <filesystem>
#include <iostream>

int main() {
    using namespace nebula;

    std::string dir = "test_wal_recovery_data";
    std::string file = "events";

    // Clean any previous run
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    LogConfig cfg{dir, file};

    {
        // First process lifetime
        NebulaLog log(cfg);

        auto o0 = log.append("first");
        auto o1 = log.append("second");
        auto o2 = log.append("third");

        assert(o0 == 0);
        assert(o1 == 1);
        assert(o2 == 2);

        auto v1 = log.read(o1);
        assert(v1.has_value());
        assert(*v1 == "second");
    } // log goes out of scope, files stay on disk

    {
        // Second process lifetime, same directory and base filename
        NebulaLog log(cfg);

        // WAL recovery should rebuild the index so we can read old entries
        auto v0 = log.read(0);
        auto v1 = log.read(1);
        auto v2 = log.read(2);

        assert(v0.has_value() && *v0 == "first");
        assert(v1.has_value() && *v1 == "second");
        assert(v2.has_value() && *v2 == "third");

        // Appending more after recovery should continue offsets
        auto o3 = log.append("fourth");
        auto o4 = log.append("fifth");

        assert(o3 == 3);
        assert(o4 == 4);

        auto v3 = log.read(3);
        auto v4 = log.read(4);

        assert(v3.has_value() && *v3 == "fourth");
        assert(v4.has_value() && *v4 == "fifth");

        // Also verify the scanner sees all five records in order
        auto it = log.scan(0);

        bool ok = it.next();
        assert(ok && it.offset() == 0 && it.value() == "first");

        ok = it.next();
        assert(ok && it.offset() == 1 && it.value() == "second");

        ok = it.next();
        assert(ok && it.offset() == 2 && it.value() == "third");

        ok = it.next();
        assert(ok && it.offset() == 3 && it.value() == "fourth");

        ok = it.next();
        assert(ok && it.offset() == 4 && it.value() == "fifth");

        ok = it.next();
        assert(!ok);
    }

    std::cout << "Phase 10 WAL recovery test passed" << std::endl;
    return 0;
}
