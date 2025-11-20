#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "nebula_log.h"

namespace nebula {

struct TopicConfig {
    std::string name;
    std::size_t num_partitions;
};

class TopicManager {
public:
    explicit TopicManager(const std::string& data_root);

    // Create a topic if it does not exist
    void create_topic(const TopicConfig& cfg);

    // Publish message to a topic using a key for partitioning.
    // Returns pair of (partition_index, logical_offset).
    std::pair<std::size_t, uint64_t> publish(
        const std::string& topic,
        const std::string& key,
        const std::string& payload);

    // Read next message for a given consumer group on a specific partition.
    // Returns empty optional if there is nothing new.
    std::optional<std::string> read_next(
        const std::string& topic,
        const std::string& consumer_group,
        std::size_t partition_index);

    // For tests and introspection: get current offset for group and partition.
    std::optional<uint64_t> current_offset(
        const std::string& topic,
        const std::string& consumer_group,
        std::size_t partition_index) const;

private:
    struct PartitionState {
        std::unique_ptr<NebulaLog> log;
    };

    struct TopicState {
        std::vector<PartitionState> partitions;
        // consumer_group -> per partition offset
        std::unordered_map<std::string, std::vector<uint64_t>> group_offsets;
    };

    std::string data_root_;
    std::unordered_map<std::string, TopicState> topics_;

    TopicState& get_or_create_topic(const TopicConfig& cfg);
    std::size_t choose_partition(const std::string& key, std::size_t num_partitions) const;
};

}  // namespace nebula
