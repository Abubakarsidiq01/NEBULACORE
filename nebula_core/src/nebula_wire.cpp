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

}  // namespace nebula
