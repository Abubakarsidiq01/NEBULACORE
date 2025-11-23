#pragma once

#include <boost/asio.hpp>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "nebula_log.h"

namespace nebula {

// Unified message type enum. Keep legacy values for PublishRequest/Response.
// New Raft-related message types are reserved here for future use.
enum class MessageType : uint8_t {
    // Existing NB wire messages
    PublishRequest  = 1,
    PublishResponse = 2,
    Heartbeat       = 3,

    // Reserved for higher-level protocol (not currently used by NebulaWireServer)
    LeaderInfo      = 4,
    Publish         = 5,

    // Raft RPCs (for future network transport)
    RaftAppendEntriesRequest  = 10,
    RaftAppendEntriesResponse = 11,
    RaftRequestVoteRequest    = 12,
    RaftRequestVoteResponse   = 13
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

// -------- Raft wire structs and encode/decode API --------

struct RaftLogEntryWire {
    uint64_t index;
    uint64_t term;
    int64_t value;
};

struct RaftAppendEntriesRequestWire {
    uint64_t term;
    uint32_t leader_id;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    uint64_t leader_commit;
    std::vector<RaftLogEntryWire> entries;
};

struct RaftAppendEntriesResponseWire {
    uint64_t term;
    bool success;
    uint64_t match_index;
};

struct RaftRequestVoteRequestWire {
    uint64_t term;
    uint32_t candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
};

struct RaftRequestVoteResponseWire {
    uint64_t term;
    bool vote_granted;
};

// Encode / Decode declarations
void encode_append_entries_request(const RaftAppendEntriesRequestWire&, std::vector<uint8_t>&);
bool decode_append_entries_request(const std::vector<uint8_t>&, RaftAppendEntriesRequestWire&);

void encode_append_entries_response(const RaftAppendEntriesResponseWire&, std::vector<uint8_t>&);
bool decode_append_entries_response(const std::vector<uint8_t>&, RaftAppendEntriesResponseWire&);

void encode_request_vote_request(const RaftRequestVoteRequestWire&, std::vector<uint8_t>&);
bool decode_request_vote_request(const std::vector<uint8_t>&, RaftRequestVoteRequestWire&);

void encode_request_vote_response(const RaftRequestVoteResponseWire&, std::vector<uint8_t>&);
bool decode_request_vote_response(const std::vector<uint8_t>&, RaftRequestVoteResponseWire&);

}  // namespace nebula
