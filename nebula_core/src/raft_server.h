#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio.hpp>

#include "raft.h"

namespace nebula {

class RaftNode;  // forward declare, defined in raft.h

struct RaftServerConfig {
    std::string host;
    uint16_t port;
};

/**
 * Raft RPC types carried over the wire.
 *
 * 1 = RequestVote
 * 2 = AppendEntries
 */
enum class RaftRpcType : uint8_t {
    RequestVote   = 1,
    AppendEntries = 2
};

/**
 * RaftServer
 *
 * Separate TCP server dedicated to Raft internal RPCs
 * (AppendEntries / RequestVote, etc.).
 *
 * The server:
 *  - accepts length-prefixed frames: [4-byte len][payload]
 *  - payload[0] = RaftRpcType
 *  - payload[1..] = encoded RPC message
 *
 * It decodes the request, calls RaftNode::handle_request_vote or
 * RaftNode::handle_append_entries, and sends back a response frame.
 */
class RaftServer {
public:
    RaftServer(const RaftServerConfig& cfg, RaftNode& node);

    void start();
    void stop();

private:
    void do_accept();

    RaftServerConfig cfg_;
    RaftNode& node_;

    boost::asio::io_context io_;
    std::unique_ptr<boost::asio::ip::tcp::acceptor> acceptor_;
    std::thread io_thread_;
    bool running_;
};

/**
 * RaftClient
 *
 * Synchronous client for sending Raft RPCs to a remote RaftServer.
 * This version is resilient:
 *  - remembers host/port
 *  - reconnects lazily if socket is closed or broken
 */
class RaftClient {
public:
    RaftClient(const std::string& host, uint16_t port);

    // Send RequestVote RPC and receive result
    RequestVoteResult request_vote(const RequestVoteRPC& req);

    // Send AppendEntries RPC and receive response
    AppendEntriesResponse append_entries(const AppendEntriesRequest& req);

private:
    using tcp = boost::asio::ip::tcp;

    std::string host_;
    uint16_t port_;

    boost::asio::io_context io_;
    tcp::socket socket_;

    // Ensure we have a connected socket. If not, resolve+connect.
    void ensure_connected();

    // Close socket on error so next call will reconnect.
    void close_socket();

    // Close and reconnect the socket.
    void reconnect();
};

} // namespace nebula
