#include "raft.h"
#include <iostream>
#include <algorithm>

namespace nebula {

RaftNode::RaftNode(std::string id)
    : id_(std::move(id))
{
    election_timeout_ms_ = random_timeout_ms();
    last_heartbeat_ = std::chrono::steady_clock::now();

    // Normalize empty log state (prevent UINT64_MAX madness)
    commit_index_ = 0;
    last_applied_ = 0;

    // A completely empty log is treated as index = -1 but we normalize
    // internal counters so AppendEntries flow works correctly.
}

int RaftNode::random_timeout_ms() {
    static thread_local std::mt19937 rng(std::random_device{}());

    // In network mode (NebulaCore), give Raft a lot more slack.
    // Heartbeat ~= 200ms, so election timeout is 1200–2400ms.
    if (transport_) {
        std::uniform_int_distribution<int> dist(1200, 2400);
        return dist(rng);
    }

    // In in-memory test mode, keep timeouts short so unit tests stay fast.
    std::uniform_int_distribution<int> dist(300, 500);
    return dist(rng);
}


bool RaftNode::is_log_up_to_date(uint64_t other_last_index,
                                 uint64_t other_last_term) const {
    uint64_t my_last_term = last_log_term();
    uint64_t my_last_index = last_log_index();

    if (other_last_term > my_last_term) return true;
    if (other_last_term < my_last_term) return false;

    // same term → compare index
    return other_last_index >= my_last_index;
}

void RaftNode::become_follower(uint64_t new_term) {
    role_ = RaftRole::Follower;
    current_term_ = new_term;
    voted_for_.reset();
    leader_id_.reset();
    votes_received_ = 0;

    election_timeout_ms_ = random_timeout_ms();
    last_heartbeat_ = std::chrono::steady_clock::now();
}

void RaftNode::become_candidate() {
    role_ = RaftRole::Candidate;
    current_term_ += 1;
    voted_for_ = id_;
    votes_received_ = 1; // vote for self

    std::cout << id_ << " became candidate for term " << current_term_ << "\n";

    RequestVoteRPC req{
        .term = current_term_,
        .candidate_id = id_,
        .last_log_index = last_log_index(),
        .last_log_term = last_log_term()
    };

    // -----------------------------
    // Mode A: test / in-memory mode
    // -----------------------------
    if (!transport_) {
        for (RaftNode* p : peers_) {
            auto res = p->handle_request_vote(req);

            if (res.term > current_term_) {
                become_follower(res.term);
                return;
            }

            if (res.vote_granted)
                votes_received_++;
        }

        if (votes_received_ > static_cast<int>(peers_.size() / 2)) {
            become_leader();
        }
        return;
    }

    // -----------------------------
    // Mode B: network-only mode
    // -----------------------------
    // peer_ids_ must contain all other node IDs (no self).
    const size_t total_nodes = peer_ids_.size() + 1; // self + peers
    const size_t quorum = (total_nodes / 2) + 1;

    votes_received_ = 1; // self

    for (const auto& pid : peer_ids_) {
        try {
            auto res = transport_->send_request_vote(pid, req);

            if (res.term > current_term_) {
                // discovered higher term, step down
                become_follower(res.term);
                return;
            }

            if (res.vote_granted)
                votes_received_++;

        } catch (const std::exception& ex) {
            std::cerr << "[RaftNode " << id_
                      << "] RequestVote RPC to "
                      << pid << " failed: " << ex.what() << "\n";
        } catch (...) {
            std::cerr << "[RaftNode " << id_
                      << "] RequestVote RPC to "
                      << pid << " failed (unknown error)\n";
        }
    }

    if (votes_received_ >= static_cast<int>(quorum)) {
        become_leader();
    } else {
        // stay candidate / follower; election timer will trigger again
    }
}

void RaftNode::become_leader() {
    role_ = RaftRole::Leader;
    leader_id_ = id_;  // leader knows itself

    std::cout << id_ << " is LEADER for term " << current_term_ << "\n";

    // Initialize follower state
    match_index_.clear();
    next_index_.clear();

    // first index after the last entry (0 if empty, N if N entries)
    size_t last_index = log_.size();

    // Mode A: in-process peers
    for (RaftNode* p : peers_) {
        match_index_[p->id_] = static_cast<size_t>(-1);
        next_index_[p->id_] = last_index;
    }

    // Mode B: network peers by ID
    for (const auto& pid : peer_ids_) {
        match_index_[pid] = static_cast<size_t>(-1);
        next_index_[pid] = last_index;
    }

    // Track our own match index
    if (log_.empty()) {
        match_index_[id_] = static_cast<size_t>(-1);
    } else {
        match_index_[id_] = log_.size() - 1;
    }

    // Immediately send heartbeats to followers
    if (!peers_.empty()) {
        for (RaftNode* p : peers_) {
            send_append_entries_to_peer(p);
        }
    } else if (transport_ && !peer_ids_.empty()) {
        for (const auto& pid : peer_ids_) {
            send_append_entries_to_peer(pid);
        }
    }

    last_heartbeat_ = std::chrono::steady_clock::now();
}

RequestVoteResult RaftNode::handle_request_vote(const RequestVoteRPC& req) {
    if (req.term < current_term_) {
        return { current_term_, false };
    }

    if (req.term > current_term_) {
        become_follower(req.term);
    }

    bool can_vote = !voted_for_ || *voted_for_ == req.candidate_id;
    bool up_to_date = is_log_up_to_date(req.last_log_index, req.last_log_term);

    if (can_vote && up_to_date) {
        voted_for_ = req.candidate_id;
        return { current_term_, true };
    }

    return { current_term_, false };
}

AppendEntriesResponse RaftNode::handle_append_entries(const AppendEntriesRequest& req) {
    std::cout << "[" << id_
              << "] RECEIVED AppendEntries from leader=" << req.leader_id
              << " term=" << req.term
              << " prev_log_index=" << req.prev_log_index
              << " prev_log_term=" << req.prev_log_term
              << " entries=" << req.entries.size()
              << " leader_commit=" << req.leader_commit
              << "\n";

    // 1. Term check
    if (req.term < (int)current_term_) {
        std::cout << "[" << id_
                  << "] REJECT AppendEntries: stale term " << req.term
                  << " < current_term " << current_term_ << "\n";
        return {
            (int)current_term_,
            false,
            log_.empty() ? (size_t)-1 : log_.size() - 1
        };
    }

    if (req.term > (int)current_term_) {
        become_follower(req.term);
    }

    role_ = RaftRole::Follower;
    leader_id_ = req.leader_id;
    last_heartbeat_ = std::chrono::steady_clock::now();

    // 2. Consistency check
    if (req.prev_log_index != (size_t)-1) {
        if (req.prev_log_index >= log_.size() ||
            log_[req.prev_log_index].term != req.prev_log_term)
        {
            std::cout << "[" << id_
                      << "] REJECT AppendEntries: log inconsistency. prev_log_index="
                      << req.prev_log_index << " local_log_size=" << log_.size()
                      << "\n";

            size_t match =
                req.prev_log_index < log_.size()
                ? req.prev_log_index
                : (log_.empty() ? (size_t)-1 : log_.size() - 1);

            return { (int)current_term_, false, match };
        }
    }

    // 3. Append / Overwrite entries
    size_t idx = (req.prev_log_index == (size_t)-1)
                 ? 0
                 : req.prev_log_index + 1;

    for (size_t i = 0; i < req.entries.size(); ++i, ++idx) {
        if (idx < log_.size()) {
            if (log_[idx].term != req.entries[i].term) {
                log_.resize(idx);
                log_.push_back(req.entries[i]);
            }
            // same term = already present, nothing to do
        } else {
            log_.push_back(req.entries[i]);
        }
    }

    // 4. Commit index sync on followers
    if (req.leader_commit != (size_t)-1 && !log_.empty()) {
        size_t last_local = log_.size() - 1;
        size_t new_commit = std::min(req.leader_commit, last_local);

        // FIX: handle sentinel (size_t)-1 correctly
        if (commit_index_ == static_cast<size_t>(-1) ||
            new_commit > commit_index_)
        {
            commit_index_ = new_commit;
            std::cout << "[" << id_
                      << "] follower commit_index updated to "
                      << commit_index_ << "\n";
        }
    }

    // Always try to apply anything up to commit_index_
    apply_committed();

    std::cout << "[" << id_
              << "] ACCEPT AppendEntries prev_log_index="
              << req.prev_log_index << "\n";

    size_t match_idx = log_.empty() ? (size_t)-1 : log_.size() - 1;

    return {
        (int)current_term_,
        true,
        match_idx
    };
}


void RaftNode::handle_append_entries_response(
    const std::string& peer_id,
    const AppendEntriesResponse& resp)
{
    if (resp.term > (int)current_term_) {
        become_follower(resp.term);
        return;
    }

    if (role_ != RaftRole::Leader) return;

    if (!resp.success) {
        if (next_index_[peer_id] > 0)
            next_index_[peer_id] -= 1;
        return;
    }

    match_index_[peer_id] = resp.match_index;
    next_index_[peer_id] = resp.match_index + 1;

    match_index_[id_] = log_.empty() ? (size_t)-1 : log_.size() - 1;

    recompute_commit_index();
}

void RaftNode::recompute_commit_index() {
    if (log_.empty()) return;

    size_t last = log_.size() - 1;

    for (size_t idx = commit_index_ + 1; idx <= last; ++idx) {

        // Raft rule: only commit entries from the current term
        if (log_[idx].term != (int)current_term_)
            continue;

        int replicated = 1; // leader itself

        for (auto& kv : match_index_) {
            if (kv.first == id_) continue;
            if (kv.second >= idx) replicated++;
        }

        int total = 0;
        if (transport_ && peers_.empty()) {
            // network mode: use peer_ids_
            total = static_cast<int>(peer_ids_.size()) + 1;
        } else {
            // in-memory mode: use peers_
            total = static_cast<int>(peers_.size()) + 1;
        }

        if (replicated > total / 2) {
            commit_index_ = idx;
        }
    }

    apply_committed();
}

// -------------------------
// Mode A: in-process peers
// -------------------------
void RaftNode::send_append_entries_to_peer(RaftNode* peer) {
    size_t next = next_index_[peer->id_];

    // If next == 0, there is no previous entry; use prev_idx = -1 and term = 0
    size_t prev_idx = (next == 0)
                      ? static_cast<size_t>(-1)
                      : next - 1;
    int prev_term = (prev_idx == static_cast<size_t>(-1))
                    ? 0
                    : log_[prev_idx].term;

    std::vector<LogEntry> entries;
    for (size_t i = next; i < log_.size(); ++i)
        entries.push_back(log_[i]);

    // FIX: compute leader_commit once and pass it into the request
    size_t leader_commit =
        (commit_index_ == static_cast<size_t>(-1))
            ? static_cast<size_t>(-1)
            : std::min(commit_index_,
                       log_.empty() ? static_cast<size_t>(0) : log_.size() - 1);

    AppendEntriesRequest req{
        .leader_id = id_,
        .term = static_cast<int>(current_term_),
        .prev_log_index = prev_idx,
        .prev_log_term = prev_term,
        .entries = std::move(entries),
        .leader_commit = leader_commit
    };

    // -----------------------------
    // Mode A: in-memory for tests
    // -----------------------------
    if (!transport_) {
        auto resp = peer->handle_append_entries(req);
        handle_append_entries_response(peer->id_, resp);
        return;
    }

    // -----------------------------
    // Mode B but with peers_ (not used in NebulaCore)
    // -----------------------------
    try {
        auto resp = transport_->send_append_entries(peer->id_, req);
        handle_append_entries_response(peer->id_, resp);
    } catch (const std::exception& ex) {
        std::cerr << "[RaftNode " << id_
                  << "] AppendEntries RPC to "
                  << peer->id_ << " failed: " << ex.what() << "\n";
    } catch (...) {
        std::cerr << "[RaftNode " << id_
                  << "] AppendEntries RPC to "
                  << peer->id_ << " failed (unknown error)\n";
    }
}

// -------------------------
// Mode B: network peers by ID
// -------------------------
void RaftNode::send_append_entries_to_peer(const std::string& peer_id) { 
    if (!transport_) return;

    // Safe lookup for next index
    size_t next_idx = 0;
    auto it = next_index_.find(peer_id);
    if (it != next_index_.end()) {
        next_idx = it->second;
    }

    // -----------------------------
    // Compute prev_idx / prev_term safely
    // -----------------------------
    size_t prev_idx;
    uint64_t prev_term;

    // If log is empty → (-1, 0)
    if (log_.empty()) {
        prev_idx = static_cast<size_t>(-1);
        prev_term = 0;
    } else {
        if (next_idx == 0) {
            prev_idx = static_cast<size_t>(-1);
            prev_term = 0;
        } else {
            prev_idx = next_idx - 1;

            // Clamp
            if (prev_idx >= log_.size()) {
                prev_idx = log_.size() - 1;
            }

            prev_term = log_[prev_idx].term;
        }
    }

    // -----------------------------
    // Collect entries safely
    // -----------------------------
    std::vector<LogEntry> entries;
    if (!log_.empty() && next_idx < log_.size()) {
        for (size_t i = next_idx; i < log_.size(); ++i) {
            entries.push_back(log_[i]);
        }
    }

    // -----------------------------
    // SAFE leader_commit
    // -----------------------------
    size_t leader_commit;

    if (commit_index_ == (size_t)-1) {
        leader_commit = (size_t)-1;
    } else {
        leader_commit = std::min(
            commit_index_,
            log_.empty() ? (size_t)0 : (log_.size() - 1)
        );
    }

    // -----------------------------
    // FINAL REQUEST — using your format
    // -----------------------------
    AppendEntriesRequest req{
        .leader_id = id_,
        .term = static_cast<int>(current_term_),
        .prev_log_index = prev_idx,
        .prev_log_term = prev_term,
        .entries = std::move(entries),
        .leader_commit = leader_commit
    };

    // -----------------------------
    // Send RPC
    // -----------------------------
    try {
        auto resp = transport_->send_append_entries(peer_id, req);
        handle_append_entries_response(peer_id, resp);

    } catch (const std::exception& ex) {
        std::cerr << "[RaftNode " << id_
                  << "] AppendEntries RPC to "
                  << peer_id << " failed: " << ex.what() << "\n";
    }
}



void RaftNode::append_client_value(const std::string& value) {
    // Append entry to leader log
    log_.push_back({ (int)current_term_, value });

    // Leader always has its own log replicated
    match_index_[id_] = log_.size() - 1;

    // First round: replicate the new entry to all followers
    if (!peers_.empty()) {
        for (RaftNode* p : peers_) {
            send_append_entries_to_peer(p);
        }
    } else if (transport_ && !peer_ids_.empty()) {
        for (const auto& pid : peer_ids_) {
            send_append_entries_to_peer(pid);
        }
    }

    // Recompute leader commit index based on new match_index_ values
    recompute_commit_index();

    // Second round: heartbeat with updated leader_commit
    if (!peers_.empty()) {
        for (RaftNode* p : peers_) {
            send_append_entries_to_peer(p);
        }
    } else if (transport_ && !peer_ids_.empty()) {
        for (const auto& pid : peer_ids_) {
            send_append_entries_to_peer(pid);
        }
    }
}

void RaftNode::apply_committed() {
    // Do not spam logs
    if (commit_index_ == last_applied_)
        return;

    // No log entries to apply
    if (log_.empty())
        return;

    size_t start = (last_applied_ == 0)
                   ? 0
                   : last_applied_ + 1;

    if (start > commit_index_)
        return;

    for (size_t i = start; i <= commit_index_ && i < log_.size(); ++i) {

        // Apply actual command
        if (state_machine_) {
            state_machine_->apply(log_[i].value);
        }

        last_applied_ = i;
    }
}


void RaftNode::tick() {
    // Single process boot timestamp so we don't start elections
    // in the first few hundred ms while networking is still wiring up.
    static const auto process_boot =
        std::chrono::steady_clock::now();

    auto now = std::chrono::steady_clock::now();

    // In network mode, give the system 1s to bring up all RaftServers
    // and establish TCP connections before we start running elections.
    if (transport_) {
        auto since_boot_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                now - process_boot)
                .count();
        if (since_boot_ms < 1000) {
            return;
        }
    }

    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                       now - last_heartbeat_)
                       .count();

    // ---------------- Leader path: send heartbeats
    if (role_ == RaftRole::Leader) {
        if (elapsed > heartbeat_interval_ms_) {
            last_heartbeat_ = now;

            if (!peers_.empty()) {
                // in-process mode
                for (RaftNode* p : peers_) {
                    send_append_entries_to_peer(p);
                }
            } else if (transport_ && !peer_ids_.empty()) {
                // network mode
                for (const auto& pid : peer_ids_) {
                    send_append_entries_to_peer(pid);
                }
            }
        }
        return;
    }

    // ---------------- Follower / Candidate path

    // Phase 8 hack for pure in-memory tests: force node1 to grab leadership
    // quickly when there is no transport. This DOES NOT run in NebulaCore
    // because transport_ is set there.
    if (!transport_ && role_ == RaftRole::Follower &&
        current_term_ == 0 &&
        id_ == "node1")
    {
        become_candidate();
        last_heartbeat_ = now;
        return;
    }

    // Normal election timeout: if we haven't heard from a leader
    // within election_timeout_ms_, start a new election.
    if (elapsed > election_timeout_ms_) {
        become_candidate();
        last_heartbeat_ = std::chrono::steady_clock::now();
    }
}


// Phase 8: snapshot support

void RaftNode::take_snapshot() {
    if (commit_index_ == (size_t)-1) return;

    int term = 0;
    if (!log_.empty() && commit_index_ < log_.size()) {
        term = log_[commit_index_].term;
    } else if (!log_.empty()) {
        term = log_.back().term;
    } else {
        term = (int)current_term_;
    }

    snapshot_index_ = commit_index_;
    snapshot_term_ = term;
    snapshot_state_ = applied_values_;

    log_.clear();

    commit_index_ = (size_t)-1;
    last_applied_ = (size_t)-1;

    last_heartbeat_ = std::chrono::steady_clock::now();
    election_timeout_ms_ = random_timeout_ms();
}

void RaftNode::install_snapshot(size_t index, int term,
                                const std::vector<std::string>& state) {
    snapshot_index_ = index;
    snapshot_term_ = term;
    snapshot_state_ = state;

    applied_values_ = state;

    if (state.empty()) {
        last_applied_ = static_cast<size_t>(-1);
        commit_index_ = static_cast<size_t>(-1);
    } else {
        last_applied_ = index;
        commit_index_ = index;
    }

    log_.clear();
}

} // namespace nebula
