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
    int heartbeat_interval_ms_ = 150;
    std::chrono::steady_clock::time_point last_heartbeat_;

    std::vector<RaftNode*> peers_;
    int votes_received_ = 0;

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

    void send_append_entries_to_peer(RaftNode* peer);
    void recompute_commit_index();
    void apply_committed();
};

} // namespace nebula
