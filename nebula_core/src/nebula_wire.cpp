#include "nebula_wire.h"

#include <boost/asio.hpp>
#include <iostream>
#include <vector>

#include "segment.h"  // for crc32

namespace nebula {

using boost::asio::ip::tcp;

static constexpr uint16_t MAGIC_NB = 0x4E42;  // "NB"
static constexpr uint8_t PROTOCOL_VERSION = 1;

class NebulaWireServer::Session
    : public std::enable_shared_from_this<NebulaWireServer::Session> {
public:
    Session(tcp::socket socket, NebulaLog* log)
        : socket_(std::move(socket)),
          log_(log) {}

    void start() {
        read_header();
    }

private:
    void read_header() {
        auto self = shared_from_this();
        boost::asio::async_read(
            socket_,
            boost::asio::buffer(&header_, sizeof(header_)),
            [this, self](boost::system::error_code ec, std::size_t /*bytes*/) {
                if (ec) {
                    return;
                }
                if (header_.magic != MAGIC_NB || header_.version != PROTOCOL_VERSION) {
                    std::cerr << "Invalid header received\n";
                    return;
                }
                body_.resize(header_.length);
                read_body();
            });
    }

    void read_body() {
        auto self = shared_from_this();
        boost::asio::async_read(
            socket_,
            boost::asio::buffer(body_.data(), body_.size()),
            [this, self](boost::system::error_code ec, std::size_t /*bytes*/) {
                if (ec) {
                    return;
                }

                uint32_t actual_crc = crc32(body_);
                if (actual_crc != header_.crc32) {
                    std::cerr << "CRC mismatch on server\n";
                    return;
                }

                handle_message();
            });
    }

    void handle_message() {
        MessageType type = static_cast<MessageType>(header_.msg_type);

        if (type == MessageType::PublishRequest) {
            if (log_) {
                log_->append(body_);
            }

            FrameHeader resp{};
            resp.magic = MAGIC_NB;
            resp.version = PROTOCOL_VERSION;
            resp.msg_type = static_cast<uint8_t>(MessageType::PublishResponse);
            resp.length = 2;
            std::string payload = "OK";
            resp.crc32 = crc32(payload);

            std::vector<boost::asio::const_buffer> bufs;
            bufs.push_back(boost::asio::buffer(&resp, sizeof(resp)));
            bufs.push_back(boost::asio::buffer(payload.data(), payload.size()));

            auto self = shared_from_this();
            boost::asio::async_write(
                socket_,
                bufs,
                [this, self](boost::system::error_code /*ec*/, std::size_t /*bytes*/) {
                    // after response we can keep the session alive and read another header
                    read_header();
                });
        } else {
            // ignore for now
        }
    }

    tcp::socket socket_;
    NebulaLog* log_;
    FrameHeader header_{};
    std::string body_;
};

NebulaWireServer::NebulaWireServer(boost::asio::io_context& io,
                                   uint16_t port,
                                   NebulaLog* log)
    : acceptor_(io, tcp::endpoint(tcp::v4(), port)),
      log_(log) {
    start_accept();
}

void NebulaWireServer::start_accept() {
    acceptor_.async_accept(
        [this](boost::system::error_code ec, tcp::socket socket) {
            if (!ec) {
                auto session = std::make_shared<Session>(std::move(socket), log_);
                session->start();
            }
            start_accept();
        });
}

// client

NebulaWireClient::NebulaWireClient(boost::asio::io_context& io,
                                   const std::string& host,
                                   const std::string& port)
    : socket_(io) {
    tcp::resolver resolver(io);
    auto endpoints = resolver.resolve(host, port);
    boost::asio::connect(socket_, endpoints);
}

void NebulaWireClient::publish(const std::string& payload) {
    FrameHeader header{};
    header.magic = MAGIC_NB;
    header.version = PROTOCOL_VERSION;
    header.msg_type = static_cast<uint8_t>(MessageType::PublishRequest);
    header.length = static_cast<uint32_t>(payload.size());
    header.crc32 = crc32(payload);

    std::vector<boost::asio::const_buffer> bufs;
    bufs.push_back(boost::asio::buffer(&header, sizeof(header)));
    bufs.push_back(boost::asio::buffer(payload.data(), payload.size()));

    boost::asio::write(socket_, bufs);

    FrameHeader resp{};
    boost::asio::read(socket_, boost::asio::buffer(&resp, sizeof(resp)));

    std::string resp_body;
    resp_body.resize(resp.length);
    boost::asio::read(socket_, boost::asio::buffer(resp_body.data(), resp_body.size()));

    uint32_t c = crc32(resp_body);
    if (c != resp.crc32) {
        std::cerr << "Client CRC mismatch on response\n";
    }
}

// -------- Raft wire encode/decode helpers --------

namespace {

// simple big-endian helpers for our Raft wire format

inline void nb_append_u8(std::vector<uint8_t>& out, uint8_t v) {
    out.push_back(v);
}

inline void nb_append_u32(std::vector<uint8_t>& out, uint32_t v) {
    out.push_back(static_cast<uint8_t>((v >> 24) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 16) & 0xFF));
    out.push_back(static_cast<uint8_t>((v >> 8) & 0xFF));
    out.push_back(static_cast<uint8_t>(v & 0xFF));
}

inline void nb_append_u64(std::vector<uint8_t>& out, uint64_t v) {
    for (int i = 7; i >= 0; --i) {
        out.push_back(static_cast<uint8_t>((v >> (i * 8)) & 0xFF));
    }
}

inline void nb_append_i64(std::vector<uint8_t>& out, int64_t v) {
    nb_append_u64(out, static_cast<uint64_t>(v));
}

inline bool nb_read_u8(const std::vector<uint8_t>& buf, std::size_t& off, uint8_t& v) {
    if (off + 1 > buf.size()) return false;
    v = buf[off++];
    return true;
}

inline bool nb_read_u32(const std::vector<uint8_t>& buf, std::size_t& off, uint32_t& v) {
    if (off + 4 > buf.size()) return false;
    v = 0;
    for (int i = 0; i < 4; ++i) {
        v = (v << 8) | buf[off++];
    }
    return true;
}

inline bool nb_read_u64(const std::vector<uint8_t>& buf, std::size_t& off, uint64_t& v) {
    if (off + 8 > buf.size()) return false;
    v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | buf[off++];
    }
    return true;
}

inline bool nb_read_i64(const std::vector<uint8_t>& buf, std::size_t& off, int64_t& v) {
    uint64_t tmp = 0;
    if (!nb_read_u64(buf, off, tmp)) return false;
    v = static_cast<int64_t>(tmp);
    return true;
}

} // namespace

void encode_append_entries_request(const RaftAppendEntriesRequestWire& r,
                                   std::vector<uint8_t>& out) {
    out.clear();
    nb_append_u64(out, r.term);
    nb_append_u32(out, r.leader_id);
    nb_append_u64(out, r.prev_log_index);
    nb_append_u64(out, r.prev_log_term);
    nb_append_u64(out, r.leader_commit);

    nb_append_u32(out, static_cast<uint32_t>(r.entries.size()));
    for (const auto& e : r.entries) {
        nb_append_u64(out, e.index);
        nb_append_u64(out, e.term);
        nb_append_i64(out, e.value);
    }
}

bool decode_append_entries_request(const std::vector<uint8_t>& buf,
    RaftAppendEntriesRequestWire& r) {
std::size_t off = 0;

if (!nb_read_u64(buf, off, r.term)) return false;

uint32_t leader_id = 0;
if (!nb_read_u32(buf, off, leader_id)) return false;
r.leader_id = leader_id;

if (!nb_read_u64(buf, off, r.prev_log_index)) return false;
if (!nb_read_u64(buf, off, r.prev_log_term)) return false;
if (!nb_read_u64(buf, off, r.leader_commit)) return false;

uint32_t count = 0;
if (!nb_read_u32(buf, off, count)) return false;

r.entries.clear();
r.entries.resize(count);

for (uint32_t i = 0; i < count; i++) {
if (!nb_read_u64(buf, off, r.entries[i].index)) return false;
if (!nb_read_u64(buf, off, r.entries[i].term)) return false;
if (!nb_read_i64(buf, off, r.entries[i].value)) return false;
}

return true;
}

} // namespace nebula

