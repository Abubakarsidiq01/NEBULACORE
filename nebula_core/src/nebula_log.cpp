#include "nebula_log.h"

#include <stdexcept>
#include <iostream>
#include <algorithm> 

namespace nebula {

    NebulaLog::NebulaLog(const LogConfig& cfg)
    : cfg_(cfg) {

    // Ensure log directory exists
    if (!cfg_.directory.empty()) {
        std::filesystem::create_directories(cfg_.directory);
    }

    // Recover existing segments and index
    recover_segments();

    // If no segments exist, create the first one
    if (segments_.empty()) {
        roll_segment();
    }

    next_logical_offset_ = offset_index_.size();
}

NebulaLog::~NebulaLog() {
    for (auto& seg : segments_) {
        if (seg.file.is_open()) {
            seg.file.close();
        }
    }
}

uint64_t NebulaLog::append(const std::string& payload) {
    const uint64_t record_bytes = sizeof(uint32_t) + payload.size();

    Segment& seg = active_segment();
    if (seg.size_bytes + record_bytes > cfg_.max_segment_bytes) {
        roll_segment();
    }

    Segment& active = active_segment();
    active.file.clear();
    active.file.seekp(0, std::ios::end);

    std::streampos pos = active.file.tellp();
    if (pos == std::streampos(-1)) {
        throw std::runtime_error("Failed to seek in segment");
    }

    uint32_t len = static_cast<uint32_t>(payload.size());
    active.file.write(reinterpret_cast<const char*>(&len), sizeof(len));
    active.file.write(payload.data(), len);
    active.file.flush();

    active.size_bytes += record_bytes;

    offset_index_.push_back(
        EntryRef{
            static_cast<uint32_t>(segments_.size() - 1),
            static_cast<uint64_t>(pos)
        });

    return next_logical_offset_++;
}

std::optional<std::string> NebulaLog::read(uint64_t logical_offset) {
    if (logical_offset >= offset_index_.size()) {
        return std::nullopt;
    }

    const EntryRef& ref = offset_index_[logical_offset];
    if (ref.seg >= segments_.size()) {
        return std::nullopt;
    }

    Segment& seg = segments_[ref.seg];
    seg.file.clear();
    seg.file.seekg(static_cast<std::streamoff>(ref.pos), std::ios::beg);
    if (!seg.file) {
        seg.file.clear();
        return std::nullopt;
    }

    uint32_t len = 0;
    seg.file.read(reinterpret_cast<char*>(&len), sizeof(len));
    if (!seg.file || len == 0) {
        seg.file.clear();
        return std::nullopt;
    }

    std::string buf(len, '\0');
    seg.file.read(buf.data(), len);
    if (!seg.file) {
        seg.file.clear();
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

uint64_t NebulaLog::file_size_bytes(const std::filesystem::path& p) {
    if (!std::filesystem::exists(p)) return 0;
    return std::filesystem::file_size(p);
}

std::string NebulaLog::segment_filename(const std::string& prefix,
                                        uint64_t base_offset) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%s_%020llu.log",
                  prefix.c_str(),
                  static_cast<unsigned long long>(base_offset));
    return std::string(buf);
}

bool NebulaLog::parse_segment_base(const std::string& name,
                                   const std::string& prefix,
                                   uint64_t& out_base) {
    if (name.rfind(prefix + "_", 0) != 0) return false;
    if (name.size() < prefix.size() + 5) return false;
    if (name.substr(name.size() - 4) != ".log") return false;

    const std::string num =
        name.substr(prefix.size() + 1,
                    name.size() - prefix.size() - 5);

    try {
        out_base = std::stoull(num);
        return true;
    } catch (...) {
        return false;
    }
}

void NebulaLog::recover_segments() {
    segments_.clear();
    offset_index_.clear();

    if (!std::filesystem::exists(cfg_.directory)) {
        return;
    }

    // Discover segment files
    for (const auto& entry : std::filesystem::directory_iterator(cfg_.directory)) {
        if (!entry.is_regular_file()) continue;

        uint64_t base = 0;
        const std::string name = entry.path().filename().string();
        if (!parse_segment_base(name, cfg_.base_filename, base)) continue;

        Segment seg;
        seg.base_offset = base;
        seg.path = entry.path();
        seg.size_bytes = file_size_bytes(seg.path);

        seg.file.open(seg.path,
                      std::ios::in | std::ios::out | std::ios::binary);
        if (!seg.file.is_open()) {
            throw std::runtime_error("Failed to open segment: " + seg.path.string());
        }

        segments_.push_back(std::move(seg));
    }

    // Sort by base offset
    std::sort(segments_.begin(), segments_.end(),
              [](const Segment& a, const Segment& b) {
                  return a.base_offset < b.base_offset;
              });

    // Scan records
    for (size_t si = 0; si < segments_.size(); ++si) {
        auto& seg = segments_[si];
        seg.file.clear();
        seg.file.seekg(0, std::ios::beg);

        while (true) {
            std::streampos pos = seg.file.tellg();
            if (pos == std::streampos(-1)) break;

            uint32_t len = 0;
            seg.file.read(reinterpret_cast<char*>(&len), sizeof(len));
            if (!seg.file || len == 0) {
                seg.file.clear();
                break;
            }

            offset_index_.push_back(
                EntryRef{static_cast<uint32_t>(si),
                         static_cast<uint64_t>(pos)});

            seg.file.seekg(len, std::ios::cur);
            if (!seg.file) {
                seg.file.clear();
                break;
            }
        }
    }
}

NebulaLog::Segment& NebulaLog::active_segment() {
    return segments_.back();
}

void NebulaLog::roll_segment() {
    Segment seg;
    seg.base_offset = next_logical_offset_;
    seg.path = std::filesystem::path(cfg_.directory) /
               segment_filename(cfg_.base_filename, seg.base_offset);

    seg.file.open(seg.path,
                  std::ios::in | std::ios::out |
                  std::ios::binary | std::ios::trunc);
    if (!seg.file.is_open()) {
        throw std::runtime_error("Failed to create segment: " + seg.path.string());
    }

    seg.size_bytes = 0;
    segments_.push_back(std::move(seg));
}

} // namespace nebula
