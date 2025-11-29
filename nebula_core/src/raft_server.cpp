#include "raft_server.h"

#include <iostream>
#include <vector>
#include <cstring>
#include <stdexcept>

#include "raft.h"

namespace nebula {

using boost::asio::ip::tcp;

// ------------ helpers for big-endian encoding/decoding -----------

static uint32_t read_u32_be(const char* p) {
    return (static_cast<uint32_t>(static_cast<unsigned char>(p[0])) << 24) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 16) |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 8)  |
           (static_cast<uint32_t>(static_cast<unsigned char>(p[3])));
}

static void write_u32_be(uint32_t v, std::vector<char>& out) {
    out.push_back(static_cast<char>((v >> 24) & 0xFF));
    out.push_back(static_cast<char>((v >> 16) & 0xFF));
    out.push_back(static_cast<char>((v >> 8) & 0xFF));
    out.push_back(static_cast<char>(v & 0xFF));
}

static uint64_t read_u64_be(const char* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v = (v << 8) | static_cast<unsigned char>(p[i]);
    }
    return v;
}

static void write_u64_be(uint64_t v, std::vector<char>& out) {
    for (int i = 7; i >= 0; --i) {
        out.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
    }
}

static uint16_t read_u16_be(const char* p) {
    return (static_cast<uint16_t>(static_cast<unsigned char>(p[0])) << 8) |
           (static_cast<uint16_t>(static_cast<unsigned char>(p[1])));
}

static void write_u16_be(uint16_t v, std::vector<char>& out) {
    out.push_back(static_cast<char>((v >> 8) & 0xFF));
    out.push_back(static_cast<char>(v & 0xFF));
}

// ------------ RPC encoding / decoding ----------------------------

namespace {

// wire format:
//
// RequestVote request:
//   [cmd=1][u64 term][u16 cand_id_len][cand_id_bytes][u64 last_log_index][u64 last_log_term]
//
// RequestVote result:
//   [cmd=1][u64 term][u8 vote_granted]
//
// AppendEntries request:
//   [cmd=2]
//   [u16 leader_id_len][leader_id_bytes]
//   [i64 term]
//   [u64 prev_log_index]
//   [i64 prev_log_term]
//   [u64 leader_commit]
//   [u32 num_entries]
//   for each entry:
//       [i64 term]
//       [u32 value_len][value_bytes]
//
// AppendEntries response:
//   [cmd=2][i64 term][u8 success][u64 match_index]

bool decode_request_vote(const std::vector<char>& buf, std::size_t pos, RequestVoteRPC& out) {
    if (pos + 8 + 2 > buf.size()) return false;

    uint64_t term = read_u64_be(buf.data() + pos);
    pos += 8;

    uint16_t id_len = read_u16_be(buf.data() + pos);
    pos += 2;
    if (pos + id_len + 8 + 8 > buf.size()) return false;

    std::string cand(buf.data() + pos, id_len);
    pos += id_len;

    uint64_t last_idx = read_u64_be(buf.data() + pos);
    pos += 8;
    uint64_t last_term = read_u64_be(buf.data() + pos);
    pos += 8;

    out.term = term;
    out.candidate_id = std::move(cand);
    out.last_log_index = last_idx;
    out.last_log_term = last_term;
    return true;
}

void encode_request_vote_result(const RequestVoteResult& res, std::vector<char>& out) {
    out.clear();
    out.push_back(static_cast<char>(RaftRpcType::RequestVote)); // cmd
    write_u64_be(res.term, out);
    out.push_back(res.vote_granted ? 1 : 0);
}

bool decode_append_entries_request(const std::vector<char>& buf, std::size_t pos,
                                   AppendEntriesRequest& out) {
    if (pos + 2 > buf.size()) return false;

    uint16_t leader_len = read_u16_be(buf.data() + pos);
    pos += 2;
    if (pos + leader_len + 8 + 8 + 8 + 4 > buf.size()) return false;

    std::string leader(buf.data() + pos, leader_len);
    pos += leader_len;

    int64_t term = static_cast<int64_t>(read_u64_be(buf.data() + pos));
    pos += 8;

    uint64_t prev_idx = read_u64_be(buf.data() + pos);
    pos += 8;

    int64_t prev_term = static_cast<int64_t>(read_u64_be(buf.data() + pos));
    pos += 8;

    uint64_t leader_commit = read_u64_be(buf.data() + pos);
    pos += 8;

    uint32_t num_entries = read_u32_be(buf.data() + pos);
    pos += 4;

    std::vector<LogEntry> entries;
    entries.reserve(num_entries);

    for (uint32_t i = 0; i < num_entries; ++i) {
        if (pos + 8 + 4 > buf.size()) return false;

        int64_t e_term = static_cast<int64_t>(read_u64_be(buf.data() + pos));
        pos += 8;

        uint32_t val_len = read_u32_be(buf.data() + pos);
        pos += 4;

        if (pos + val_len > buf.size()) return false;
        std::string value(buf.data() + pos, val_len);
        pos += val_len;

        LogEntry e;
        e.term = static_cast<int>(e_term);
        e.value = std::move(value);
        entries.push_back(std::move(e));
    }

    out.leader_id = std::move(leader);
    out.term = static_cast<int>(term);
    out.prev_log_index = (prev_idx == static_cast<uint64_t>(-1)) ? static_cast<size_t>(-1)
                                                                 : static_cast<size_t>(prev_idx);
    out.prev_log_term = static_cast<int>(prev_term);
    out.entries = std::move(entries);
    out.leader_commit = (leader_commit == static_cast<uint64_t>(-1))
                          ? static_cast<size_t>(-1)
                          : static_cast<size_t>(leader_commit);
    return true;
}

void encode_append_entries_response(const AppendEntriesResponse& resp, std::vector<char>& out) {
    out.clear();
    out.push_back(static_cast<char>(RaftRpcType::AppendEntries)); // cmd
    write_u64_be(static_cast<uint64_t>(resp.term), out);
    out.push_back(resp.success ? 1 : 0);
    write_u64_be(static_cast<uint64_t>(resp.match_index), out);
}

} // namespace

// ----------------- RaftSession (server side) ---------------------

class RaftSession : public std::enable_shared_from_this<RaftSession> {
public:
    RaftSession(tcp::socket socket, RaftNode& node)
        : socket_(std::move(socket)),
          node_(node),
          read_header_(4) {}

    void start() {
        read_header();
    }

private:
    void read_header() {
        auto self = shared_from_this();
        boost::asio::async_read(
            socket_,
            boost::asio::buffer(read_header_),
            [this, self](const boost::system::error_code& ec, std::size_t /*bytes*/) {
                if (ec) {
                    return;
                }
                uint32_t len = read_u32_be(read_header_.data());
                if (len == 0 || len > 1024 * 1024) {
                    return;
                }
                read_body_.resize(len);
                read_body();
            });
    }

    void read_body() {
        auto self = shared_from_this();
        boost::asio::async_read(
            socket_,
            boost::asio::buffer(read_body_),
            [this, self](const boost::system::error_code& ec, std::size_t /*bytes*/) {
                if (ec) {
                    return;
                }
                handle_request();
            });
    }

    void handle_request() {
        if (read_body_.empty()) {
            read_header();
            return;
        }

        uint8_t cmd = static_cast<uint8_t>(read_body_[0]);

        if (cmd == static_cast<uint8_t>(RaftRpcType::RequestVote)) {
            handle_request_vote();
        } else if (cmd == static_cast<uint8_t>(RaftRpcType::AppendEntries)) {
            handle_append_entries();
        } else {
            // Unknown RPC, ignore and continue
            read_header();
        }
    }

    void handle_request_vote() {
        RequestVoteRPC req{};
        if (!decode_request_vote(read_body_, 1, req)) {
            read_header();
            return;
        }

        auto res = node_.handle_request_vote(req);

        std::vector<char> payload;
        encode_request_vote_result(res, payload);
        send_frame(payload);
    }

    void handle_append_entries() {
        AppendEntriesRequest req{};
        if (!decode_append_entries_request(read_body_, 1, req)) {
            read_header();
            return;
        }

        auto res = node_.handle_append_entries(req);

        std::vector<char> payload;
        encode_append_entries_response(res, payload);
        send_frame(payload);
    }

    void send_frame(const std::vector<char>& payload) {
        std::vector<char> frame;
        write_u32_be(static_cast<uint32_t>(payload.size()), frame);
        frame.insert(frame.end(), payload.begin(), payload.end());

        auto self = shared_from_this();
        boost::asio::async_write(
            socket_,
            boost::asio::buffer(frame),
            [this, self](const boost::system::error_code& ec, std::size_t /*bytes*/) {
                if (ec) {
                    return;
                }
                // ready for next RPC
                read_header();
            });
    }

    tcp::socket socket_;
    RaftNode& node_;
    std::vector<char> read_header_;
    std::vector<char> read_body_;
};

// ----------------- RaftServer implementation --------------------

RaftServer::RaftServer(const RaftServerConfig& cfg, RaftNode& node)
    : cfg_(cfg),
      node_(node),
      io_(),
      acceptor_(nullptr),
      io_thread_(),
      running_(false) {}

void RaftServer::start() {
    if (running_) {
        return;
    }
    running_ = true;

    boost::system::error_code ec;
    auto addr = boost::asio::ip::make_address(cfg_.host, ec);
    if (ec) {
        std::cerr << "RaftServer address error: " << ec.message() << "\n";
        return;
    }

    tcp::endpoint ep(addr, cfg_.port);
    acceptor_ = std::make_unique<tcp::acceptor>(io_);

    acceptor_->open(ep.protocol(), ec);
    if (ec) {
        std::cerr << "RaftServer open error: " << ec.message() << "\n";
        return;
    }

    acceptor_->set_option(tcp::acceptor::reuse_address(true), ec);
    if (ec) {
        std::cerr << "RaftServer reuse_address error: " << ec.message() << "\n";
        return;
    }

    acceptor_->bind(ep, ec);
    if (ec) {
        std::cerr << "RaftServer bind error: " << ec.message() << "\n";
        return;
    }

    acceptor_->listen(boost::asio::socket_base::max_listen_connections, ec);
    if (ec) {
        std::cerr << "RaftServer listen error: " << ec.message() << "\n";
        return;
    }

    do_accept();

    io_thread_ = std::thread([this]() {
        try {
            io_.run();
        } catch (const std::exception& ex) {
            std::cerr << "RaftServer io_context exception: " << ex.what() << "\n";
        }
    });
}

void RaftServer::stop() {
    if (!running_) {
        return;
    }
    running_ = false;

    boost::system::error_code ec;
    if (acceptor_) {
        acceptor_->close(ec);
    }

    io_.stop();

    if (io_thread_.joinable()) {
        io_thread_.join();
    }
}

void RaftServer::do_accept() {
    if (!acceptor_ || !running_) {
        return;
    }

    auto socket = std::make_shared<tcp::socket>(io_);
    acceptor_->async_accept(
        *socket,
        [this, socket](const boost::system::error_code& ec) {
            if (!ec && running_) {
                auto session = std::make_shared<RaftSession>(std::move(*socket), node_);
                session->start();
            }
            if (running_) {
                do_accept();
            }
        });
}

// ----------------- RaftClient implementation --------------------

RaftClient::RaftClient(const std::string& host, uint16_t port)
    : host_(host),
      port_(port),
      io_(),
      socket_(io_) {
    // Do not connect immediately; connect lazily on first RPC.
}

void RaftClient::close_socket() {
    if (socket_.is_open()) {
        boost::system::error_code ec;
        socket_.close(ec);
    }
}

void RaftClient::reconnect() {
    // Close previous socket
    boost::system::error_code ec;
    socket_.close(ec);

    // Resolve host -> address
    auto addr = boost::asio::ip::make_address(host_, ec);
    if (ec) {
        std::cerr << "[RaftClient] reconnect: invalid host " << host_
                  << " error=" << ec.message() << "\n";
        return;
    }

    boost::asio::ip::tcp::endpoint ep(addr, port_);

    // Start async connect
    socket_.async_connect(
        ep,
        [this](const boost::system::error_code& ec2) {
            if (!ec2) {
                std::cout << "[RaftClient] reconnected to "
                          << host_ << ":" << port_ << "\n";
            } else {
                std::cerr << "[RaftClient] reconnect failed: "
                          << ec2.message() << "\n";
            }
        }
    );
}


void RaftClient::ensure_connected() {
    if (socket_.is_open()) {
        return;
    }

    boost::system::error_code ec;
    tcp::resolver resolver(io_);
    auto endpoints = resolver.resolve(host_, std::to_string(port_), ec);
    if (ec) {
        throw std::runtime_error("RaftClient resolve error: " + ec.message());
    }

    tcp::socket new_socket(io_);
    boost::asio::connect(new_socket, endpoints, ec);
    if (ec) {
        throw std::runtime_error("RaftClient connect error: " + ec.message());
    }

    socket_ = std::move(new_socket);
}

RequestVoteResult RaftClient::request_vote(const RequestVoteRPC& req) {
    ensure_connected();

    std::vector<char> payload;
    payload.push_back(static_cast<char>(RaftRpcType::RequestVote));
    write_u64_be(req.term, payload);

    uint16_t id_len = static_cast<uint16_t>(req.candidate_id.size());
    write_u16_be(id_len, payload);
    payload.insert(payload.end(), req.candidate_id.begin(), req.candidate_id.end());

    write_u64_be(req.last_log_index, payload);
    write_u64_be(req.last_log_term, payload);

    std::vector<char> frame;
    write_u32_be(static_cast<uint32_t>(payload.size()), frame);
    frame.insert(frame.end(), payload.begin(), payload.end());

    boost::system::error_code ec;

    boost::asio::write(socket_, boost::asio::buffer(frame), ec);
    if (ec) {
        close_socket();
        throw std::runtime_error("RaftClient RequestVote write error: " + ec.message());
    }

    // read response header
    char hdr[4];
    boost::asio::read(socket_, boost::asio::buffer(hdr, 4), ec);
    if (ec) {
        close_socket();
        throw std::runtime_error("RaftClient RequestVote read header error: " + ec.message());
    }

    uint32_t len = read_u32_be(hdr);
    std::vector<char> resp(len);
    boost::asio::read(socket_, boost::asio::buffer(resp.data(), resp.size()), ec);
    if (ec) {
        close_socket();
        throw std::runtime_error("RaftClient RequestVote read body error: " + ec.message());
    }

    if (resp.empty() || resp[0] != static_cast<char>(RaftRpcType::RequestVote)) {
        close_socket();
        throw std::runtime_error("RaftClient: bad RequestVote response");
    }

    if (resp.size() < 1 + 8 + 1) {
        close_socket();
        throw std::runtime_error("RaftClient: short RequestVote response");
    }

    RequestVoteResult out{};
    std::size_t pos = 1;
    out.term = read_u64_be(resp.data() + pos);
    pos += 8;

    uint8_t granted = static_cast<uint8_t>(resp[pos]);
    out.vote_granted = (granted != 0);

    return out;
}

AppendEntriesResponse RaftClient::append_entries(const AppendEntriesRequest& req) {
    ensure_connected();

    std::vector<char> payload;
    payload.push_back(static_cast<char>(RaftRpcType::AppendEntries));

    uint16_t leader_len = static_cast<uint16_t>(req.leader_id.size());
    write_u16_be(leader_len, payload);
    payload.insert(payload.end(), req.leader_id.begin(), req.leader_id.end());

    write_u64_be(static_cast<uint64_t>(req.term), payload);

    uint64_t prev_idx = (req.prev_log_index == static_cast<size_t>(-1))
                          ? static_cast<uint64_t>(-1)
                          : static_cast<uint64_t>(req.prev_log_index);
    write_u64_be(prev_idx, payload);

    write_u64_be(static_cast<uint64_t>(req.prev_log_term), payload);

    uint64_t leader_commit = (req.leader_commit == static_cast<size_t>(-1))
                               ? static_cast<uint64_t>(-1)
                               : static_cast<uint64_t>(req.leader_commit);
    write_u64_be(leader_commit, payload);

    uint32_t num_entries = static_cast<uint32_t>(req.entries.size());
    write_u32_be(num_entries, payload);
    for (const auto& e : req.entries) {
        write_u64_be(static_cast<uint64_t>(e.term), payload);
        uint32_t val_len = static_cast<uint32_t>(e.value.size());
        write_u32_be(val_len, payload);
        payload.insert(payload.end(), e.value.begin(), e.value.end());
    }

    std::vector<char> frame;
    write_u32_be(static_cast<uint32_t>(payload.size()), frame);
    frame.insert(frame.end(), payload.begin(), payload.end());

    boost::system::error_code ec;

    boost::asio::write(socket_, boost::asio::buffer(frame), ec);
    if (ec) {
        close_socket();
        throw std::runtime_error("RaftClient AppendEntries write error: " + ec.message());
    }

    // read response header
    char hdr[4];
    boost::asio::read(socket_, boost::asio::buffer(hdr, 4), ec);
    if (ec) {
        close_socket();
        throw std::runtime_error("RaftClient AppendEntries read header error: " + ec.message());
    }

    uint32_t len = read_u32_be(hdr);
    std::vector<char> resp(len);
    boost::asio::read(socket_, boost::asio::buffer(resp.data(), resp.size()), ec);
    if (ec) {
        std::cerr << "[RaftClient] AppendEntries read error: "
                  << ec.message() << " (retrying)\n";
        reconnect();
        return append_entries(req);
    }

    if (resp.empty() || resp[0] != static_cast<char>(RaftRpcType::AppendEntries)) {
        close_socket();
        throw std::runtime_error("RaftClient: bad AppendEntries response");
    }

    if (resp.size() < 1 + 8 + 1 + 8) {
        close_socket();
        throw std::runtime_error("RaftClient: short AppendEntries response");
    }

    AppendEntriesResponse out{};
    std::size_t pos = 1;
    out.term = static_cast<int>(read_u64_be(resp.data() + pos));
    pos += 8;

    uint8_t success = static_cast<uint8_t>(resp[pos]);
    pos += 1;
    out.success = (success != 0);

    out.match_index = static_cast<size_t>(read_u64_be(resp.data() + pos));
    pos += 8;

    return out;
}

} // namespace nebula
