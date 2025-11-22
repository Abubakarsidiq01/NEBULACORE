#include "nebula_log.h"

#include <stdexcept>
#include <iostream>

namespace nebula {

NebulaLog::NebulaLog(const LogConfig& cfg) {
    // Build full path: directory / base_filename
    std::filesystem::path dir_path(cfg.directory);
    if (!cfg.directory.empty()) {
        std::filesystem::create_directories(dir_path);
    }

    path_ = dir_path / cfg.base_filename;

    const bool existed = std::filesystem::exists(path_);

    // Try to open for read/write. If it fails (for example file does not exist), create it
    file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
    if (!file_.is_open()) {
        // Create a new empty file
        file_.clear();
        file_.open(path_, std::ios::out | std::ios::binary);
        if (!file_.is_open()) {
            throw std::runtime_error("Failed to create log file: " + path_.string());
        }
        file_.close();

        // Reopen in read/write mode
        file_.open(path_, std::ios::in | std::ios::out | std::ios::binary);
        if (!file_.is_open()) {
            throw std::runtime_error("Failed to open log file: " + path_.string());
        }
    }

    if (existed) {
        recover_index();
    } else {
        offset_index_.clear();
        next_logical_offset_ = 0;
        file_.clear();
        file_.seekp(0, std::ios::end);
    }
}

NebulaLog::~NebulaLog() {
    if (file_.is_open()) {
        file_.close();
    }
}

void NebulaLog::recover_index() {
    offset_index_.clear();
    file_.clear();
    file_.seekg(0, std::ios::beg);

    uint64_t logical = 0;
    while (true) {
        std::streampos pos = file_.tellg();
        if (pos == std::streampos(-1)) {
            break;
        }

        uint32_t len = 0;
        file_.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!file_) {
            // Either EOF or a read error. Clear flags and stop
            file_.clear();
            break;
        }

        // Zero length would be weird. Treat it as stop
        if (len == 0) {
            break;
        }

        // Record starting position for this logical offset
        offset_index_.push_back(static_cast<uint64_t>(pos));

        // Skip payload bytes
        file_.seekg(len, std::ios::cur);
        if (!file_) {
            file_.clear();
            break;
        }

        ++logical;
    }

    next_logical_offset_ = logical;

    // Prepare for appends
    file_.clear();
    file_.seekp(0, std::ios::end);
}

uint64_t NebulaLog::append(const std::string& payload) {
    file_.clear();
    file_.seekp(0, std::ios::end);

    std::streampos pos = file_.tellp();
    if (pos == std::streampos(-1)) {
        throw std::runtime_error("Failed to seek in log file: " + path_.string());
    }

    uint32_t len = static_cast<uint32_t>(payload.size());
    file_.write(reinterpret_cast<const char*>(&len), sizeof(len));
    file_.write(payload.data(), len);
    if (!file_) {
        throw std::runtime_error("Failed to write to log file: " + path_.string());
    }
    file_.flush();

    uint64_t logical = next_logical_offset_++;
    if (logical >= offset_index_.size()) {
        offset_index_.push_back(static_cast<uint64_t>(pos));
    } else {
        offset_index_[logical] = static_cast<uint64_t>(pos);
    }

    return logical;
}

std::optional<std::string> NebulaLog::read(uint64_t logical_offset) {
    if (logical_offset >= next_logical_offset_) {
        return std::nullopt;
    }
    if (logical_offset >= offset_index_.size()) {
        return std::nullopt;
    }

    uint64_t pos = offset_index_[logical_offset];
    file_.clear();
    file_.seekg(static_cast<std::streamoff>(pos), std::ios::beg);
    if (!file_) {
        file_.clear();
        return std::nullopt;
    }

    uint32_t len = 0;
    file_.read(reinterpret_cast<char*>(&len), sizeof(len));
    if (!file_ || len == 0) {
        file_.clear();
        return std::nullopt;
    }

    std::string buf(len, '\0');
    file_.read(buf.data(), len);
    if (!file_) {
        file_.clear();
        return std::nullopt;
    }

    return buf;
}

NebulaLog::Iterator NebulaLog::scan(uint64_t start_offset) {
    return Iterator(this, start_offset);
}

NebulaLog::Iterator::Iterator(NebulaLog* log, uint64_t start_offset)
    : log_(log),
      current_offset_(start_offset),
      current_value_(),
      initialized_(false) {}

bool NebulaLog::Iterator::next() {
    if (!log_) {
        return false;
    }
    auto val = log_->read(current_offset_);
    if (!val.has_value()) {
        return false;
    }
    current_value_ = *val;
    ++current_offset_;
    initialized_ = true;
    return true;
}

uint64_t NebulaLog::Iterator::offset() const {
    // After a successful next(), current_offset_ has been incremented
    // so the current record offset is current_offset_ minus one
    if (!initialized_) {
        return 0;
    }
    return current_offset_ - 1;
}

const std::string& NebulaLog::Iterator::value() const {
    return current_value_;
}

} // namespace nebula
