#pragma once
#include <cstdint>
#include <vector>
#include <string>
#include <optional>
#include <chrono>
#include <random>
#include <unordered_map>

namespace nebula {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct LogEntry {
    int term;
    std::string value;
};

struct RequestVoteRPC {
    uint64_t term;
    std::string candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
};

struct RequestVoteResult {
    uint64_t term;
    bool vote_granted;
};

struct AppendEntriesRequest {
    std::string leader_id;
    int term;

    // index and term of log entry immediately preceding new ones
    // if prev_log_index == (size_t)-1, it means "no previous entry"
    size_t prev_log_index;
    int prev_log_term;

    std::vector<LogEntry> entries;
    // (size_t)-1 means "no commit info"
    size_t leader_commit;
};

struct AppendEntriesResponse {
    int term;
    bool success;
    size_t match_index; // highest index that now matches on follower
};

// ----------------------------------------------------------
// Network transport interface for Raft RPCs (Mode B)
// ----------------------------------------------------------
class IRaftTransport {
public:
    // Send RequestVote to a node identified by target_id and get its response
    virtual RequestVoteResult send_request_vote(
        const std::string& target_id,
        const RequestVoteRPC& rpc) = 0;

    // Send AppendEntries to a node identified by target_id and get its response
    virtual AppendEntriesResponse send_append_entries(
        const std::string& target_id,
        const AppendEntriesRequest& rpc) = 0;

    virtual ~IRaftTransport() = default;
};
class IRaftStateMachine {
public:
    virtual ~IRaftStateMachine() {}
    virtual void apply(const std::string& command) = 0;
};

class RaftNode {
public:
    explicit RaftNode(std::string id);

    // timers and state transitions
    void tick();
    void become_follower(uint64_t new_term);
    void become_candidate();
    void become_leader();

    // RPC entrypoints
    RequestVoteResult handle_request_vote(const RequestVoteRPC& req);
    AppendEntriesResponse handle_append_entries(const AppendEntriesRequest& req);
    void handle_append_entries_response(const std::string& peer_id,
                                        const AppendEntriesResponse& resp);

    // leader side API used by tests
    void append_client_value(const std::string& value);

    // cluster wiring
    void add_peer(RaftNode* peer) { peers_.push_back(peer); }

    // inspection for tests
    RaftRole role() const { return role_; }
    uint64_t current_term() const { return current_term_; }
    std::optional<std::string> leader_id() const { return leader_id_; }

    size_t log_size() const { return log_.size(); }
    const LogEntry& log_at(size_t idx) const { return log_.at(idx); }

    size_t commit_index() const { return commit_index_; }

    // Phase 6: applied state machine values
    const std::vector<std::string>& applied_values() const {
        return applied_values_;
    }

    // Mode B: network transport + peer ids
    void set_transport(IRaftTransport* t) { transport_ = t; }

    // Optional state machine that will be driven by committed log entries.
    void set_state_machine(IRaftStateMachine* sm) { state_machine_ = sm; }


    // List of peer node IDs used in network mode; excludes self.
    void set_peer_ids(const std::vector<std::string>& ids) {
        peer_ids_ = ids;
    }

    // Phase 8: snapshot API
    void take_snapshot();
    void install_snapshot(size_t index, int term,
                          const std::vector<std::string>& state);

    size_t snapshot_index() const { return snapshot_index_; }
    int snapshot_term() const { return snapshot_term_; }
    const std::vector<std::string>& snapshot_state() const {
        return snapshot_state_;
    }

private:
    std::string id_;

    // persistent state
    uint64_t current_term_ = 0;
    std::optional<std::string> voted_for_;
    std::vector<LogEntry> log_; // 0 based

    // volatile state
    size_t commit_index_ = static_cast<size_t>(-1);
    size_t last_applied_ = static_cast<size_t>(-1);
    std::vector<std::string> applied_values_;

    // per follower state (only valid on leader)
    std::unordered_map<std::string, size_t> next_index_;
    std::unordered_map<std::string, size_t> match_index_;

    // metadata
    RaftRole role_ = RaftRole::Follower;
    std::optional<std::string> leader_id_;

    // timers
    int election_timeout_ms_;
    int heartbeat_interval_ms_ = 200;
    std::chrono::steady_clock::time_point last_heartbeat_;

    std::vector<RaftNode*> peers_;
    int votes_received_ = 0;

    // Phase 8: snapshot metadata
    size_t snapshot_index_ = static_cast<size_t>(-1);
    int snapshot_term_ = 0;
    std::vector<std::string> snapshot_state_;

    int random_timeout_ms();

    uint64_t last_log_index() const {
        if (log_.empty()) return static_cast<uint64_t>(-1);
        return static_cast<uint64_t>(log_.size() - 1);
    }
    
    uint64_t last_log_term() const {
        if (log_.empty()) return 0;
        return static_cast<uint64_t>(log_.back().term);
    }
    

    bool is_log_up_to_date(uint64_t other_last_index,
                           uint64_t other_last_term) const;

    // Mode A: in-process peers
    void send_append_entries_to_peer(RaftNode* peer);

    // Mode B: network peers by ID
    void send_append_entries_to_peer(const std::string& peer_id);

    void recompute_commit_index();
    void apply_committed();

    // Mode B: network transport (nullptr in tests)
    IRaftTransport* transport_ = nullptr;

    // Mode B: peer IDs used when transport_ is set
    std::vector<std::string> peer_ids_;

    // Optional external state machine (NebulaNode) to apply committed commands.
    IRaftStateMachine* state_machine_ = nullptr;

    
};

} // namespace nebula
