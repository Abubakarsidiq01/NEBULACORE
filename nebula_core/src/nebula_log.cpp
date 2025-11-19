#include "nebula_log.h"
#include "segment.h"

#include <filesystem>
#include <stdexcept>
#include <unordered_map>

namespace nebula {

struct NebulaLog::Impl {
    LogConfig cfg;
    Segment segment;
    uint64_t next_logical_offset;
    uint64_t next_file_pos;  // where the next append will write
    std::unordered_map<uint64_t, uint64_t> offset_to_file_pos;

    Impl(const LogConfig& config)
        : cfg(config),
          segment(config.directory + "/" + config.base_filename, /*create_new=*/true),
          next_logical_offset(0),
          next_file_pos(0) {
    }
};

NebulaLog::NebulaLog(const LogConfig& cfg)
    : impl_(std::make_unique<Impl>(cfg)) {}

NebulaLog::~NebulaLog() = default;

uint64_t NebulaLog::append(const std::string& payload) {
    uint64_t logical = impl_->next_logical_offset;
    uint64_t file_pos = impl_->segment.append(logical, payload);

    impl_->offset_to_file_pos[logical] = file_pos;
    impl_->next_logical_offset += 1;
    impl_->next_file_pos = impl_->segment.size_bytes();
    return logical;
}

std::optional<std::string> NebulaLog::read(uint64_t logical_offset) {
    auto it = impl_->offset_to_file_pos.find(logical_offset);
    if (it == impl_->offset_to_file_pos.end()) {
        return std::nullopt;
    }
    return impl_->segment.read_at_file_pos(it->second);
}

NebulaLog::Iterator NebulaLog::scan(uint64_t start_offset) {
    return Iterator(this, start_offset);
}

// Iterator

NebulaLog::Iterator::Iterator(NebulaLog* log, uint64_t start_offset)
    : log_(log),
      current_offset_(start_offset),
      initialized_(false) {}

bool NebulaLog::Iterator::next() {
    if (!log_) return false;

    auto val = log_->read(current_offset_);
    if (!val.has_value()) {
        return false;
    }
    current_value_ = *val;
    initialized_ = true;
    current_offset_ += 1;
    return true;
}

uint64_t NebulaLog::Iterator::offset() const {
    return current_offset_ - 1;
}

const std::string& NebulaLog::Iterator::value() const {
    return current_value_;
}

}  // namespace nebula
