#pragma once

#include <boost/asio.hpp>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>
#include <chrono>

namespace nebula {

struct NodeId {
    std::string id;
    std::string host;
    uint16_t gossip_port;
};

struct MemberView {
    std::string id;
    uint64_t heartbeat;
    std::chrono::milliseconds age_ms;
};

class GossipCluster {
public:
    GossipCluster(boost::asio::io_context& io,
                  const NodeId& self,
                  const std::vector<NodeId>& peers);

    void start();
    void stop();

    // Snapshot of known members, safe to call from test thread
    std::vector<MemberView> members_snapshot() const;

private:
    void start_receive();
    void handle_receive(const boost::system::error_code& ec, std::size_t bytes);
    void schedule_gossip();
    void send_gossip();

    void apply_heartbeat(const std::string& node_id, uint64_t hb);

    struct MemberState {
        uint64_t heartbeat;
        std::chrono::steady_clock::time_point last_seen;
    };

    boost::asio::io_context& io_;
    NodeId self_;
    std::vector<NodeId> peers_;

    mutable std::mutex mtx_;
    std::unordered_map<std::string, MemberState> members_;

    boost::asio::ip::udp::socket socket_;
    boost::asio::ip::udp::endpoint remote_endpoint_;
    std::array<char, 1024> recv_buffer_;

    boost::asio::steady_timer timer_;
    bool running_;
    uint64_t local_heartbeat_;
};

}  // namespace nebula
