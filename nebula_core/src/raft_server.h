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
 * Simple synchronous client for sending Raft RPCs to a remote RaftServer.
 * Creates a new TCP connection per client instance.
 *
 * Usage:
 *   RaftClient c("127.0.0.1", 6001);
 *   RequestVoteResult res = c.request_vote(req);
 *   AppendEntriesResponse ar = c.append_entries(ae);
 */
class RaftClient {
public:
    RaftClient(const std::string& host, uint16_t port);

    // Send RequestVote RPC and receive result
    RequestVoteResult request_vote(const RequestVoteRPC& req);

    // Send AppendEntries RPC and receive response
    AppendEntriesResponse append_entries(const AppendEntriesRequest& req);

private:
    boost::asio::io_context io_;
    boost::asio::ip::tcp::socket socket_;
};

} // namespace nebula
