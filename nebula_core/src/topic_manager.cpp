#include "topic_manager.h"

#include <filesystem>
#include <functional>
#include <stdexcept>

namespace fs = std::filesystem;

namespace nebula {

TopicManager::TopicManager(const std::string& data_root)
    : data_root_(data_root) {
    fs::create_directories(data_root_);
}

void TopicManager::create_topic(const TopicConfig& cfg) {
    get_or_create_topic(cfg);
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
        log_cfg.base_filename = "segment_00000000.log";

        state.partitions[i].log = std::make_unique<NebulaLog>(log_cfg);
    }

    auto [insert_it, _] = topics_.emplace(cfg.name, std::move(state));
    return insert_it->second;
}

std::size_t TopicManager::choose_partition(const std::string& key, std::size_t num_partitions) const {
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

}  // namespace nebula
