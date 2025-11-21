#include "../src/nebula_node.h"
#include "../src/node_server.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iostream>
#include <thread>
#include <vector>

#include <boost/asio.hpp>

using boost::asio::ip::tcp;

static uint32_t to_u32_be(uint32_t v) {
    return ((v & 0x000000FF) << 24) |
           ((v & 0x0000FF00) << 8)  |
           ((v & 0x00FF0000) >> 8)  |
           ((v & 0xFF000000) >> 24);
}

static void write_u32_be(uint32_t v, std::vector<char>& out) {
    out.push_back(static_cast<char>((v >> 24) & 0xFF));
    out.push_back(static_cast<char>((v >> 16) & 0xFF));
    out.push_back(static_cast<char>((v >> 8) & 0xFF));
    out.push_back(static_cast<char>(v & 0xFF));
}

static uint32_t read_u32_be(const char* p) {
    return (static_cast<uint32_t>(static_cast<unsigned char>(p[0])) << 24) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 16) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 8)  |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[3])));
}

static void write_u16_be(uint16_t v, std::vector<char>& out) {
    out.push_back(static_cast<char>((v >> 8) & 0xFF));
    out.push_back(static_cast<char>(v & 0xFF));
}

static uint16_t read_u16_be(const char* p) {
    return (static_cast<uint16_t>(static_cast<unsigned char>(p[0])) << 8) |
           (static_cast<uint16_t>(static_cast<unsigned char>(p[1])));
}

int main() {
    using namespace nebula;

    std::filesystem::remove_all("node1_data");
    std::filesystem::remove_all("node2_data");
    std::filesystem::remove_all("node3_data");

    NodeId n1{"node1", "127.0.0.1", 9301};
    NodeId n2{"node2", "127.0.0.1", 9302};
    NodeId n3{"node3", "127.0.0.1", 9303};

    NebulaNodeConfig c1;
    c1.data_root = "node1_data";
    c1.self = n1;
    c1.peers = {n2, n3};
    c1.default_partitions = 3;

    NebulaNodeConfig c2;
    c2.data_root = "node2_data";
    c2.self = n2;
    c2.peers = {n1, n3};
    c2.default_partitions = 3;

    NebulaNodeConfig c3;
    c3.data_root = "node3_data";
    c3.self = n3;
    c3.peers = {n1, n2};
    c3.default_partitions = 3;

    NebulaNode node1(c1);
    NebulaNode node2(c2);
    NebulaNode node3(c3);

    node1.start();
    node2.start();
    node3.start();

    std::this_thread::sleep_for(std::chrono::seconds(4));

    auto l1 = node1.leader_id();
    auto l2 = node2.leader_id();
    auto l3 = node3.leader_id();

    std::cout << "Leaders from nodes: "
              << (l1 ? *l1 : "none") << ", "
              << (l2 ? *l2 : "none") << ", "
              << (l3 ? *l3 : "none") << "\n";

    assert(l1.has_value());
    assert(l2.has_value());
    assert(l3.has_value());
    assert(*l1 == *l2);
    assert(*l2 == *l3);

    std::string leader_id = *l1;

    NebulaNode* leader_node = nullptr;
    if (leader_id == c1.self.id) {
        leader_node = &node1;
    } else if (leader_id == c2.self.id) {
        leader_node = &node2;
    } else if (leader_id == c3.self.id) {
        leader_node = &node3;
    } else {
        assert(false && "unknown leader id");
    }

    std::cout << "Cluster leader resolved to: " << leader_id << "\n";

    NodeServerConfig scfg;
    scfg.host = "127.0.0.1";
    scfg.port = 9501;

    NodeServer server(scfg, *leader_node);
    server.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    boost::asio::io_context io;
    tcp::socket sock(io);

    tcp::endpoint ep(boost::asio::ip::make_address("127.0.0.1"), scfg.port);
    sock.connect(ep);

    auto do_request = [&](const std::vector<char>& payload) -> std::vector<char> {
        std::vector<char> frame;
        write_u32_be(static_cast<uint32_t>(payload.size()), frame);
        frame.insert(frame.end(), payload.begin(), payload.end());

        boost::asio::write(sock, boost::asio::buffer(frame));

        std::array<char, 4> hdr{};
        boost::asio::read(sock, boost::asio::buffer(hdr));
        uint32_t len = read_u32_be(hdr.data());
        std::vector<char> body(len);
        boost::asio::read(sock, boost::asio::buffer(body));
        return body;
    };

    // 1. LEADER_INFO
    {
        std::vector<char> payload;
        payload.push_back(static_cast<char>(1)); // command id

        auto resp = do_request(payload);
        assert(!resp.empty());
        uint8_t status = static_cast<uint8_t>(resp[0]);
        assert(status == 0);

        uint16_t id_len = read_u16_be(resp.data() + 1);
        assert(1 + 2 + id_len <= resp.size());
        std::string id(resp.data() + 3, id_len);

        std::cout << "Server reports leader: " << id << "\n";
        assert(id == leader_id);
    }

    // 2. PUBLISH
    uint32_t returned_partition = 0;
    {
        std::string topic = "alerts";
        std::string key = "deviceA";
        std::string msg = "hello_from_client";

        std::vector<char> payload;
        payload.push_back(static_cast<char>(2)); // PUBLISH

        write_u16_be(static_cast<uint16_t>(topic.size()), payload);
        payload.insert(payload.end(), topic.begin(), topic.end());

        write_u16_be(static_cast<uint16_t>(key.size()), payload);
        payload.insert(payload.end(), key.begin(), key.end());

        write_u32_be(static_cast<uint32_t>(msg.size()), payload);
        payload.insert(payload.end(), msg.begin(), msg.end());

        auto resp = do_request(payload);
        assert(!resp.empty());
        uint8_t status = static_cast<uint8_t>(resp[0]);
        assert(status == 0); // ok

        assert(resp.size() >= 1 + 4 + 8);
        returned_partition = read_u32_be(resp.data() + 1);
        std::cout << "Publish returned partition " << returned_partition << "\n";
    }

    // 3. CONSUME
    {
        std::string topic = "alerts";
        std::string group = "cg1";

        std::vector<char> payload;
        payload.push_back(static_cast<char>(3)); // CONSUME

        write_u16_be(static_cast<uint16_t>(topic.size()), payload);
        payload.insert(payload.end(), topic.begin(), topic.end());

        write_u16_be(static_cast<uint16_t>(group.size()), payload);
        payload.insert(payload.end(), group.begin(), group.end());

        write_u32_be(returned_partition, payload);

        auto resp = do_request(payload);
        assert(!resp.empty());
        uint8_t status = static_cast<uint8_t>(resp[0]);
        assert(status == 0); // ok

        uint32_t msg_len = read_u32_be(resp.data() + 1);
        assert(1 + 4 + msg_len <= resp.size());
        std::string body(resp.data() + 5, msg_len);

        std::cout << "Consumed payload: " << body << "\n";
        assert(body == "hello_from_client");

        auto resp2 = do_request(payload);
        uint8_t status2 = static_cast<uint8_t>(resp2[0]);
        assert(status2 == 1); // empty
    }

    sock.close();
    server.stop();

    node1.stop();
    node2.stop();
    node3.stop();

    std::cout << "Node server integration test passed\n";
    return 0;
}
