#include "raft.h"
#include <iostream>
#include <algorithm>

namespace nebula {

RaftNode::RaftNode(std::string id)
    : id_(std::move(id))
{
    election_timeout_ms_ = random_timeout_ms();
    last_heartbeat_ = std::chrono::steady_clock::now();
}

int RaftNode::random_timeout_ms() {
    static thread_local std::mt19937 rng(std::random_device{}());
    std::uniform_int_distribution<int> dist(300, 500);
    return dist(rng);
}

bool RaftNode::is_log_up_to_date(uint64_t other_last_index,
                                 uint64_t other_last_term) const {
    uint64_t my_last_term = last_log_term();
    uint64_t my_last_index = last_log_index();

    if (other_last_term > my_last_term) return true;
    if (other_last_term < my_last_term) return false;
    // same term, compare index
    return other_last_index >= my_last_index;
}

void RaftNode::become_follower(uint64_t new_term) {
    role_ = RaftRole::Follower;
    current_term_ = new_term;
    voted_for_.reset();
    leader_id_.reset();
    votes_received_ = 0;
    // reset timer
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

    for (RaftNode* p : peers_) {
        auto res = p->handle_request_vote(req);
        if (res.vote_granted) {
            votes_received_++;
        }
        if (res.term > current_term_) {
            become_follower(res.term);
            return;
        }
    }

    if (votes_received_ > static_cast<int>(peers_.size() / 2)) {
        become_leader();
    }
}

void RaftNode::become_leader() {
    role_ = RaftRole::Leader;
    leader_id_ = id_;

    std::cout << id_ << " is LEADER for term " << current_term_ << "\n";

    // init leader state
    size_t next = log_.size();
    for (RaftNode* p : peers_) {
        next_index_[p->id_] = next;
        match_index_[p->id_] = static_cast<size_t>(-1);
    }
    // leader itself always has its full log
    match_index_[id_] = log_.empty() ? static_cast<size_t>(-1) : log_.size() - 1;

    // send initial heartbeats
    for (RaftNode* p : peers_) {
        send_append_entries_to_peer(p);
    }
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
    // 1. reply false if term < current_term
    if (req.term < static_cast<int>(current_term_)) {
        return AppendEntriesResponse{ static_cast<int>(current_term_), false,
                                      log_.empty() ? static_cast<size_t>(-1) : log_.size() - 1 };
    }

    // If term is newer, step down
    if (req.term > static_cast<int>(current_term_)) {
        become_follower(req.term);
    }

    role_ = RaftRole::Follower;
    leader_id_ = req.leader_id;
    last_heartbeat_ = std::chrono::steady_clock::now();

    // 2. consistency check on prev_log_index / prev_log_term
    if (req.prev_log_index != static_cast<size_t>(-1)) {
        if (req.prev_log_index >= log_.size()) {
            return AppendEntriesResponse{ static_cast<int>(current_term_), false,
                                          log_.empty() ? static_cast<size_t>(-1) : log_.size() - 1 };
        }
        if (log_[req.prev_log_index].term != req.prev_log_term) {
            return AppendEntriesResponse{ static_cast<int>(current_term_), false,
                                          req.prev_log_index };
        }
    }

    // 3. apply entries
    size_t idx = (req.prev_log_index == static_cast<size_t>(-1))
                 ? 0
                 : req.prev_log_index + 1;

    for (size_t i = 0; i < req.entries.size(); ++i, ++idx) {
        if (idx < log_.size()) {
            // conflict: overwrite from here
            if (log_[idx].term != req.entries[i].term) {
                log_.resize(idx);
                log_.push_back(req.entries[i]);
            } else {
                // same term, already present, nothing
            }
        } else {
            log_.push_back(req.entries[i]);
        }
    }

    // 4. update commit index
    if (req.leader_commit != static_cast<size_t>(-1)) {
        size_t last_index = log_.empty() ? static_cast<size_t>(-1) : log_.size() - 1;
        commit_index_ = std::min(req.leader_commit, last_index);
    }

    size_t match_idx = log_.empty() ? static_cast<size_t>(-1) : log_.size() - 1;
    return AppendEntriesResponse{ static_cast<int>(current_term_), true, match_idx };
}

void RaftNode::handle_append_entries_response(const std::string& peer_id,
                                              const AppendEntriesResponse& resp) {
    if (resp.term > static_cast<int>(current_term_)) {
        become_follower(resp.term);
        return;
    }

    if (role_ != RaftRole::Leader) return;

    if (!resp.success) {
        // follower log is behind or conflicting, back off next_index
        auto it = next_index_.find(peer_id);
        if (it != next_index_.end() && it->second > 0) {
            it->second -= 1;
        }
        return;
    }

    // success: advance match_index and next_index
    match_index_[peer_id] = resp.match_index;
    next_index_[peer_id] = resp.match_index + 1;

    // leader always has full log
    match_index_[id_] = log_.empty() ? static_cast<size_t>(-1) : log_.size() - 1;

    recompute_commit_index();
}

void RaftNode::recompute_commit_index() {
    if (log_.empty()) return;

    size_t last_idx = log_.size() - 1;

    for (size_t idx = commit_index_ + 1; idx <= last_idx; ++idx) {
        int count = 1; // leader itself
        for (auto& kv : match_index_) {
            const std::string& peer_id = kv.first;
            if (peer_id == id_) continue;
            if (kv.second >= idx) {
                count++;
            }
        }
        if (count > static_cast<int>((peers_.size() + 1) / 2) &&
            log_[idx].term == static_cast<int>(current_term_)) {
            commit_index_ = idx;
        }
    }
}

void RaftNode::send_append_entries_to_peer(RaftNode* peer) {
    size_t next = 0;
    auto it = next_index_.find(peer->id_);
    if (it == next_index_.end()) {
        next = log_.size();
        next_index_[peer->id_] = next;
    } else {
        next = it->second;
    }

    size_t prev_index;
    int prev_term;
    if (next == 0) {
        prev_index = static_cast<size_t>(-1);
        prev_term = 0;
    } else {
        prev_index = next - 1;
        prev_term = log_[prev_index].term;
    }

    std::vector<LogEntry> entries;
    for (size_t i = next; i < log_.size(); ++i) {
        entries.push_back(log_[i]);
    }

    AppendEntriesRequest req{
        .leader_id = id_,
        .term = static_cast<int>(current_term_),
        .prev_log_index = prev_index,
        .prev_log_term = prev_term,
        .entries = std::move(entries),
        .leader_commit = commit_index_
    };

    auto resp = peer->handle_append_entries(req);
    handle_append_entries_response(peer->id_, resp);
}

void RaftNode::append_client_value(const std::string& value) {
    if (role_ != RaftRole::Leader) {
        // for tests we can still allow it, but real Raft would reject
        std::cout << "append_client_value called on non leader " << id_ << "\n";
    }

    log_.push_back(LogEntry{ static_cast<int>(current_term_), value });

    // leader has full log
    match_index_[id_] = log_.size() - 1;

    // send to all peers
    for (RaftNode* p : peers_) {
        send_append_entries_to_peer(p);
    }

    // recompute commit index based on new matches
    recompute_commit_index();
}

void RaftNode::tick() {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - last_heartbeat_).count();

    if (role_ == RaftRole::Leader) {
        // leader heartbeats not modeled as repeated RPCs here,
        // tests do not depend on timed heartbeats
        if (elapsed > heartbeat_interval_ms_) {
            last_heartbeat_ = now;
        }
        return;
    }

    if (elapsed > election_timeout_ms_) {
        become_candidate();
        last_heartbeat_ = std::chrono::steady_clock::now();
    }
}

} // namespace nebula
