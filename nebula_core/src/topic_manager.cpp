#include "topic_manager.h"

#include <filesystem>
#include <functional>
#include <stdexcept>
#include <fstream>
#include <sstream>

namespace fs = std::filesystem;

namespace nebula {

TopicManager::TopicManager(const std::string& data_root)
    : data_root_(data_root) {
    fs::create_directories(data_root_);
}

void TopicManager::create_topic(const TopicConfig& cfg) {
    get_or_create_topic(cfg);
}

void TopicManager::publish_from_raft(const std::string& topic,
                                     const std::string& key,
                                     const std::string& payload) {
    // Use the same default topic config as publish() (3 partitions).
    TopicConfig cfg{topic, 3};
    create_topic(cfg);

    // Append locally. This does not perform any cross-node replication
    // and does not talk to Raft; it is only called after a Raft log
    // entry has already been committed.
    (void)publish(topic, key, payload);
}

TopicManager::TopicState& TopicManager::get_or_create_topic(const TopicConfig& cfg) {
    auto it = topics_.find(cfg.name);
    if (it != topics_.end()) {
        return it->second;
    }

    TopicState state;
    state.partitions.resize(cfg.num_partitions);

    for (std::size_t i = 0; i < cfg.num_partitions; ++i) {
        std::string dir = data_root_ + "/" + cfg.name + "_p" + std::to_string(i);
        fs::create_directories(dir);

        LogConfig log_cfg;
        log_cfg.directory = dir;
        log_cfg.base_filename = "segment";
        log_cfg.max_segment_bytes = 8 * 1024 * 1024; // 8MB for now


        PartitionState part;
        part.log = std::make_unique<NebulaLog>(log_cfg);
        state.partitions[i] = std::move(part);
    }

    auto [insert_it, _] = topics_.emplace(cfg.name, std::move(state));
    load_group_offsets(cfg.name, insert_it->second);
    return insert_it->second;
}

std::size_t TopicManager::choose_partition(const std::string& key,
                                           std::size_t num_partitions) const {
    if (num_partitions == 0) {
        throw std::runtime_error("No partitions configured");
    }
    std::size_t h = std::hash<std::string>{}(key);
    return h % num_partitions;
}

std::pair<std::size_t, uint64_t> TopicManager::publish(
    const std::string& topic,
    const std::string& key,
    const std::string& payload) {

    // default topic config: 3 partitions if not created explicitly
    TopicConfig cfg{topic, 3};
    TopicState& state = get_or_create_topic(cfg);

    std::size_t pidx = choose_partition(key, state.partitions.size());
    PartitionState& part = state.partitions[pidx];

    if (!part.log) {
        throw std::runtime_error("Partition log not initialized");
    }

    uint64_t offset = part.log->append(payload);
    return {pidx, offset};
}

std::optional<std::string> TopicManager::read_next(
    const std::string& topic,
    const std::string& consumer_group,
    std::size_t partition_index) {

    auto tit = topics_.find(topic);
    if (tit == topics_.end()) {
        return std::nullopt;
    }
    TopicState& state = tit->second;

    if (partition_index >= state.partitions.size()) {
        return std::nullopt;
    }

    auto git = state.group_offsets.find(consumer_group);
    if (git == state.group_offsets.end()) {
        // initialize offsets for this group
        std::vector<uint64_t> offsets(state.partitions.size(), 0);
        auto [new_it, _] = state.group_offsets.emplace(consumer_group, std::move(offsets));
        git = new_it;
    }

    std::vector<uint64_t>& offsets = git->second;
    uint64_t& current_offset = offsets[partition_index];

    PartitionState& part = state.partitions[partition_index];
    auto val = part.log->read(current_offset);
    if (!val.has_value()) {
        return std::nullopt;
    }

    // advance offset for this group and partition
    current_offset += 1;
    save_group_offsets(topic, state);
    return val;
}

std::optional<uint64_t> TopicManager::current_offset(
    const std::string& topic,
    const std::string& consumer_group,
    std::size_t partition_index) const {

    auto tit = topics_.find(topic);
    if (tit == topics_.end()) {
        return std::nullopt;
    }
    const TopicState& state = tit->second;

    auto git = state.group_offsets.find(consumer_group);
    if (git == state.group_offsets.end()) {
        return std::nullopt;
    }

    const std::vector<uint64_t>& offsets = git->second;
    if (partition_index >= offsets.size()) {
        return std::nullopt;
    }
    return offsets[partition_index];
}

std::string TopicManager::group_state_path(const std::string& topic) const {
    // One metadata file per topic:
    //   <data_root_>/<topic>_groups.meta
    return data_root_ + "/" + topic + "_groups.meta";
}

void TopicManager::load_group_offsets(const std::string& topic, TopicState& state) {
    const std::string path = group_state_path(topic);

    std::ifstream in(path);
    if (!in.is_open()) {
        // No metadata yet (first run or no consumers)
        return;
    }

    // Format per line:
    //   group_name offset0 offset1 offset2 ...
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string group;
        if (!(iss >> group)) {
            continue;
        }

        std::vector<uint64_t> offsets;
        uint64_t off = 0;
        while (iss >> off) {
            offsets.push_back(off);
        }

        // If the topic has grown more partitions, pad with zeros.
        if (offsets.size() < state.partitions.size()) {
            offsets.resize(state.partitions.size(), 0);
        }
        // If the topic has fewer partitions than stored, truncate.
        if (offsets.size() > state.partitions.size()) {
            offsets.resize(state.partitions.size());
        }

        state.group_offsets[group] = std::move(offsets);
    }
}

void TopicManager::save_group_offsets(const std::string& topic,
                                      const TopicState& state) const {
    const std::string path = group_state_path(topic);

    std::ofstream out(path, std::ios::trunc);
    if (!out.is_open()) {
        // Best-effort; if this fails, consumer offsets remain in memory only.
        return;
    }

    // Write each consumer group's offsets on one line:
    //   group_name offset0 offset1 offset2 ...
    for (const auto& kv : state.group_offsets) {
        const std::string& group = kv.first;
        const std::vector<uint64_t>& offsets = kv.second;

        out << group;
        for (uint64_t off : offsets) {
            out << ' ' << off;
        }
        out << '\n';
    }
}

}  // namespace nebula
