#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include <boost/asio.hpp>

#include "nebula_node.h"

namespace nebula {

struct NodeServerConfig {
    std::string host;
    uint16_t port;
};

class NodeServer {
public:
    NodeServer(const NodeServerConfig& cfg, NebulaNode& node);

    void start();
    void stop();

private:
    void do_accept();

    NodeServerConfig cfg_;
    NebulaNode& node_;

    boost::asio::io_context io_;
    std::unique_ptr<boost::asio::ip::tcp::acceptor> acceptor_;
    std::thread io_thread_;
    bool running_;
};

}  // namespace nebula
