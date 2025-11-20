#include "cluster.h"

#include <cstring>
#include <iostream>

namespace nebula {

using boost::asio::ip::udp;

static constexpr int GOSSIP_INTERVAL_MS = 500;
static constexpr int FAILURE_TIMEOUT_MS = 5000;

GossipCluster::GossipCluster(boost::asio::io_context& io,
                             const NodeId& self,
                             const std::vector<NodeId>& peers)
    : io_(io),
      self_(self),
      peers_(peers),
      socket_(io),
      timer_(io),
      running_(false),
      local_heartbeat_(0) {
    udp::endpoint local_ep(udp::v4(), self_.gossip_port);
    socket_.open(udp::v4());
    socket_.bind(local_ep);

    // register self in members map
    MemberState self_state;
    self_state.heartbeat = 0;
    self_state.last_seen = std::chrono::steady_clock::now();
    members_.emplace(self_.id, self_state);
}

void GossipCluster::start() {
    if (running_) return;
    running_ = true;
    start_receive();
    schedule_gossip();
}

void GossipCluster::stop() {
    if (!running_) return;
    running_ = false;
    boost::system::error_code ec;
    socket_.close(ec);
    timer_.cancel();
}

void GossipCluster::start_receive() {
    socket_.async_receive_from(
        boost::asio::buffer(recv_buffer_),
        remote_endpoint_,
        [this](const boost::system::error_code& ec, std::size_t bytes) {
            handle_receive(ec, bytes);
        });
}

void GossipCluster::handle_receive(const boost::system::error_code& ec, std::size_t bytes) {
    if (ec || !running_) {
        return;
    }

    if (bytes < sizeof(uint16_t) + sizeof(uint64_t)) {
        start_receive();
        return;
    }

    const char* data = recv_buffer_.data();
    std::size_t pos = 0;

    uint16_t id_len;
    std::memcpy(&id_len, data + pos, sizeof(id_len));
    pos += sizeof(id_len);

    if (pos + id_len + sizeof(uint64_t) > bytes) {
        start_receive();
        return;
    }

    std::string node_id(data + pos, id_len);
    pos += id_len;

    uint64_t hb;
    std::memcpy(&hb, data + pos, sizeof(hb));
    pos += sizeof(hb);

    apply_heartbeat(node_id, hb);

    start_receive();
}

void GossipCluster::schedule_gossip() {
    if (!running_) return;

    timer_.expires_after(std::chrono::milliseconds(GOSSIP_INTERVAL_MS));
    timer_.async_wait([this](const boost::system::error_code& ec) {
        if (!ec && running_) {
            send_gossip();
            schedule_gossip();
        }
    });
}

void GossipCluster::send_gossip() {
    local_heartbeat_ += 1;

    // Update self state
    {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = members_.find(self_.id);
        if (it != members_.end()) {
            it->second.heartbeat = local_heartbeat_;
            it->second.last_seen = std::chrono::steady_clock::now();
        } else {
            MemberState st;
            st.heartbeat = local_heartbeat_;
            st.last_seen = std::chrono::steady_clock::now();
            members_.emplace(self_.id, st);
        }
    }

    // Serialize: [uint16 id_len][id bytes][uint64 heartbeat]
    uint16_t id_len = static_cast<uint16_t>(self_.id.size());
    std::string buf;
    buf.resize(sizeof(id_len) + id_len + sizeof(uint64_t));
    std::size_t pos = 0;
    std::memcpy(buf.data() + pos, &id_len, sizeof(id_len));
    pos += sizeof(id_len);
    std::memcpy(buf.data() + pos, self_.id.data(), id_len);
    pos += id_len;
    std::memcpy(buf.data() + pos, &local_heartbeat_, sizeof(local_heartbeat_));
    pos += sizeof(local_heartbeat_);

    for (const auto& peer : peers_) {
        udp::endpoint ep(boost::asio::ip::make_address(peer.host), peer.gossip_port);
        socket_.async_send_to(
            boost::asio::buffer(buf),
            ep,
            [](const boost::system::error_code& /*ec*/, std::size_t /*bytes*/) {});
    }
}

void GossipCluster::apply_heartbeat(const std::string& node_id, uint64_t hb) {
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(mtx_);
    auto it = members_.find(node_id);
    if (it == members_.end()) {
        MemberState st;
        st.heartbeat = hb;
        st.last_seen = now;
        members_.emplace(node_id, st);
    } else {
        if (hb > it->second.heartbeat) {
            it->second.heartbeat = hb;
            it->second.last_seen = now;
        }
    }
}

std::vector<MemberView> GossipCluster::members_snapshot() const {
    auto now = std::chrono::steady_clock::now();
    std::vector<MemberView> out;

    std::lock_guard<std::mutex> lock(mtx_);
    for (const auto& kv : members_) {
        const auto& id = kv.first;
        const auto& state = kv.second;
        auto age_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - state.last_seen);

        if (age_ms.count() > FAILURE_TIMEOUT_MS) {
            continue;
        }

        MemberView v;
        v.id = id;
        v.heartbeat = state.heartbeat;
        v.age_ms = age_ms;
        out.push_back(v);
    }
    return out;
}

}  // namespace nebula
