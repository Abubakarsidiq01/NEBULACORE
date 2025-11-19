#include "../src/nebula_log.h"
#include "../src/nebula_wire.h"

#include <boost/asio.hpp>
#include <cassert>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <thread>

int main() {
    using namespace nebula;

    std::string dir = "test_logs_wire";
    std::filesystem::create_directories(dir);
    std::string file = "segment_00000000.log";
    std::filesystem::remove(dir + "/" + file);

    LogConfig cfg{dir, file};
    NebulaLog log(cfg);

    boost::asio::io_context server_io;

    NebulaWireServer server(server_io, 5555, &log);

    std::thread server_thread([&server_io]() {
        server_io.run();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    boost::asio::io_context client_io;
    NebulaWireClient client(client_io, "127.0.0.1", "5555");

    client.publish("hello over nebula wire");
    client.publish("second message");

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto v0 = log.read(0);
    auto v1 = log.read(1);

    assert(v0.has_value());
    assert(v1.has_value());
    assert(*v0 == "hello over nebula wire");
    assert(*v1 == "second message");

    server_io.stop();
    client_io.stop();
    server_thread.join();

    std::cout << "Phase 2 wire test passed\n";
    return 0;
}
