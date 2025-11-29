#include "node_server.h"

#include <array>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <optional>
#include <vector>

namespace nebula {

using boost::asio::ip::tcp;

// ----------------------------
// Big-endian helpers
// ----------------------------
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

static uint16_t read_u16_be(const char* p) {
    return (static_cast<uint16_t>(static_cast<unsigned char>(p[0])) << 8) |
           (static_cast<uint16_t>(static_cast<unsigned char>(p[1])));
}

static void write_u16_be(uint16_t v, std::vector<char>& out) {
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

// ----------------------------
// Per-connection Session
// ----------------------------
class Session : public std::enable_shared_from_this<Session> {
public:
    Session(tcp::socket socket, NebulaNode& node)
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
                    // Drop garbage frames
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

        const char* data = read_body_.data();
        std::size_t size = read_body_.size();
        uint8_t cmd = static_cast<uint8_t>(data[0]);

        switch (cmd) {
        case 1: // LEADER_INFO
            handle_leader_info();
            break;
        case 2: // PUBLISH
            handle_publish(data + 1, size - 1);
            break;
        case 3: // CONSUME
            handle_consume(data + 1, size - 1);
            break;
        default:
            send_error_response("unknown command");
            break;
        }
    }

    // ----------------------------
    // Command 1: LEADER_INFO
    // ----------------------------
    void handle_leader_info() {
        std::vector<char> payload;
        uint8_t status = 0;

        auto lid = node_.leader_id();
        if (!lid.has_value()) {
            status = 1;
            payload.push_back(static_cast<char>(status));
            std::string msg = "no leader";
            write_u16_be(static_cast<uint16_t>(msg.size()), payload);
            payload.insert(payload.end(), msg.begin(), msg.end());
        } else {
            payload.push_back(static_cast<char>(status));
            const std::string& leader = *lid;
            write_u16_be(static_cast<uint16_t>(leader.size()), payload);
            payload.insert(payload.end(), leader.begin(), leader.end());
        }

        send_frame(payload);
    }

    // ----------------------------
    // Command 2: PUBLISH
    //
    // Frame payload (after cmd byte):
    //   [topic_len:u16][topic bytes]
    //   [key_len:u16][key bytes]
    //   [payload_len:u32][payload bytes]
    //
    // Response:
    //   status=0: [0][partition:u32][offset:u64]
    //   status=1: redirect [1][host_len:u16][host bytes][port:u16]
    //   status=2: error    [2][msg_len:u16][msg bytes]
    // ----------------------------
    void handle_publish(const char* data, std::size_t size) {
        std::size_t pos = 0;
        if (size < 2) {
            send_error_response("bad publish frame");
            return;
        }

        // topic
        uint16_t topic_len = read_u16_be(data + pos);
        pos += 2;
        if (pos + topic_len + 2 > size) {
            send_error_response("bad publish topic length");
            return;
        }
        std::string topic(data + pos, topic_len);
        pos += topic_len;

        // key
        uint16_t key_len = read_u16_be(data + pos);
        pos += 2;
        if (pos + key_len + 4 > size) {
            send_error_response("bad publish key length");
            return;
        }
        std::string key(data + pos, key_len);
        pos += key_len;

        // payload
        if (pos + 4 > size) {
            send_error_response("bad publish payload header");
            return;
        }
        uint32_t payload_len = read_u32_be(data + pos);
        pos += 4;
        if (pos + payload_len > size) {
            send_error_response("bad publish payload length");
            return;
        }
        std::string body(data + pos, payload_len);
        pos += payload_len;

        std::vector<char> payload;

        // Not leader → redirect
        if (!node_.is_leader()) {
            payload.push_back(static_cast<char>(1)); // redirect

            auto [host, port] = node_.leader_address();
            if (host.empty() || port == 0) {
                // leader unknown → hard error
                payload.clear();
                payload.push_back(static_cast<char>(2)); // error
                std::string msg = "leader unknown";
                write_u16_be(static_cast<uint16_t>(msg.size()), payload);
                payload.insert(payload.end(), msg.begin(), msg.end());
                send_frame(payload);
                return;
            }

            write_u16_be(static_cast<uint16_t>(host.size()), payload);
            payload.insert(payload.end(), host.begin(), host.end());
            write_u16_be(port, payload);

            send_frame(payload);
            return;
        }

        // Leader path
        try {
            auto res = node_.publish(topic, key, body);
            uint32_t partition = static_cast<uint32_t>(res.first);
            uint64_t offset = res.second;

            payload.push_back(static_cast<char>(0)); // ok
            write_u32_be(partition, payload);
            write_u64_be(offset, payload);
            send_frame(payload);
        } catch (const std::exception& ex) {
            std::string msg = ex.what();
            payload.push_back(static_cast<char>(2)); // error
            write_u16_be(static_cast<uint16_t>(msg.size()), payload);
            payload.insert(payload.end(), msg.begin(), msg.end());
            send_frame(payload);
        }
    }

    // ----------------------------
    // Command 3: CONSUME
    //
    // Frame payload (after cmd byte):
    //   [topic_len:u16][topic bytes]
    //   [group_len:u16][group bytes]
    //   [partition_idx:u32]
    //
    // Response:
    //   status=0: [0][msg_len:u32][msg bytes]
    //   status=1: [1] (no message)
    //   status=2: [2][msg_len:u16][msg bytes]
    // ----------------------------
    void handle_consume(const char* data, std::size_t size) {
        std::size_t pos = 0;
        if (size < 2) {
            send_error_response("bad consume frame");
            return;
        }

        // topic
        uint16_t topic_len = read_u16_be(data + pos);
        pos += 2;
        if (pos + topic_len + 2 > size) {
            send_error_response("bad consume topic length");
            return;
        }
        std::string topic(data + pos, topic_len);
        pos += topic_len;

        // group
        uint16_t group_len = read_u16_be(data + pos);
        pos += 2;
        if (pos + group_len + 4 > size) {
            send_error_response("bad consume group length");
            return;
        }
        std::string group(data + pos, group_len);
        pos += group_len;

        // partition index
        if (pos + 4 > size) {
            send_error_response("bad consume partition index");
            return;
        }
        uint32_t partition_idx = read_u32_be(data + pos);
        pos += 4;

        std::vector<char> payload;

        auto msg = node_.consume(topic, group, static_cast<std::size_t>(partition_idx));
        if (!msg.has_value()) {
            payload.push_back(static_cast<char>(1)); // empty
            send_frame(payload);
            return;
        }

        payload.push_back(static_cast<char>(0)); // ok
        write_u32_be(static_cast<uint32_t>(msg->size()), payload);
        payload.insert(payload.end(), msg->begin(), msg->end());
        send_frame(payload);
    }

    // ----------------------------
    // Common helpers
    // ----------------------------
    void send_error_response(const std::string& msg) {
        std::vector<char> payload;
        payload.push_back(static_cast<char>(2)); // error
        write_u16_be(static_cast<uint16_t>(msg.size()), payload);
        payload.insert(payload.end(), msg.begin(), msg.end());
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
                // Ready for next request
                read_header();
            });
    }

    tcp::socket socket_;
    NebulaNode& node_;
    std::vector<char> read_header_;
    std::vector<char> read_body_;
};

// ----------------------------
// NodeServer
// ----------------------------
NodeServer::NodeServer(const NodeServerConfig& cfg, NebulaNode& node)
    : cfg_(cfg),
      node_(node),
      io_(),
      acceptor_(nullptr),
      io_thread_(),
      running_(false) {}

      void NodeServer::start() { 
        if (running_) {
            return;
        }
        running_ = true;
    
        boost::system::error_code ec;
        auto addr = boost::asio::ip::make_address(cfg_.host, ec);
        if (ec) {
            std::cerr << "NodeServer address error: " << ec.message() << "\n";
            return;
        }
    
        tcp::endpoint ep(addr, cfg_.port);
        acceptor_ = std::make_unique<tcp::acceptor>(io_);
    
        // open
        acceptor_->open(ep.protocol(), ec);
        if (ec) {
            std::cerr << "NodeServer open error: " << ec.message() << "\n";
            return;
        }
    
        // reuse address
        acceptor_->set_option(tcp::acceptor::reuse_address(true), ec);
        if (ec) {
            std::cerr << "NodeServer reuse_address error: " << ec.message() << "\n";
            return;
        }
    
        // *** bind with error logging ***
        acceptor_->bind(ep, ec);
        if (ec) {
            std::cerr << "NodeServer bind error: " << ec.message()
                      << " on " << cfg_.host << ":" << cfg_.port << "\n";
            return;
        }
    
        // listen
        acceptor_->listen(boost::asio::socket_base::max_listen_connections, ec);
        if (ec) {
            std::cerr << "NodeServer listen error: " << ec.message() << "\n";
            return;
        }
    
        do_accept();
    
        io_thread_ = std::thread([this]() {
            try {
                io_.run();
            } catch (const std::exception& ex) {
                std::cerr << "NodeServer io_context exception: " << ex.what() << "\n";
            }
        });
    }
    
void NodeServer::stop() {
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

void NodeServer::do_accept() {
    if (!acceptor_ || !running_) {
        return;
    }

    auto socket = std::make_shared<tcp::socket>(io_);
    acceptor_->async_accept(
        *socket,
        [this, socket](const boost::system::error_code& ec) {
            if (!ec && running_) {
                auto session = std::make_shared<Session>(std::move(*socket), node_);
                session->start();
            }
            if (running_) {
                do_accept();
            }
        });
}

} // namespace nebula