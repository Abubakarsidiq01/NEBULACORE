#pragma once

#include <boost/asio.hpp>
#include <cstdint>
#include <memory>
#include <string>

#include "nebula_log.h"

namespace nebula {

enum class MessageType : uint8_t {
    PublishRequest  = 1,
    PublishResponse = 2,
    Heartbeat       = 3
};

struct FrameHeader {
    uint16_t magic;
    uint8_t version;
    uint8_t msg_type;
    uint32_t length;
    uint32_t crc32;
};

class NebulaWireServer {
public:
    NebulaWireServer(boost::asio::io_context& io,
                     uint16_t port,
                     NebulaLog* log);

private:
    void start_accept();

    class Session;
    boost::asio::ip::tcp::acceptor acceptor_;
    NebulaLog* log_;
};

class NebulaWireClient {
public:
    NebulaWireClient(boost::asio::io_context& io,
                     const std::string& host,
                     const std::string& port);

    // Blocking publish for Phase 2
    void publish(const std::string& payload);

private:
    boost::asio::ip::tcp::socket socket_;
};

}  // namespace nebula
