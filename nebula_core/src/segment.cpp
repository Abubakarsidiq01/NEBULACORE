#include "segment.h"

#include <stdexcept>
#include <filesystem>

namespace nebula {

Segment::Segment(const std::string& path, bool create_new)
    : path_(path) {
    std::ios::openmode mode = std::ios::binary | std::ios::in | std::ios::out;
    if (create_new) {
        mode |= std::ios::trunc;
    }
    file_.open(path_, mode);
    if (!file_.is_open()) {
        throw std::runtime_error("Failed to open segment file: " + path_);
    }
}

uint64_t Segment::append(uint64_t logical_offset, const std::string& payload) {
    file_.seekp(0, std::ios::end);
    uint64_t pos = static_cast<uint64_t>(file_.tellp());

    RecordHeader header;
    header.size = static_cast<uint32_t>(payload.size());
    header.offset = logical_offset;
    header.crc32 = crc32(payload);

    file_.write(reinterpret_cast<const char*>(&header), sizeof(header));
    file_.write(payload.data(), payload.size());
    file_.flush();

    return pos;
}

std::optional<std::string> Segment::read_at_file_pos(uint64_t file_pos) {
    file_.seekg(static_cast<std::streamoff>(file_pos), std::ios::beg);

    RecordHeader header;
    file_.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (!file_) {
        return std::nullopt;
    }

    std::string payload;
    payload.resize(header.size);
    file_.read(payload.data(), header.size);
    if (!file_) {
        return std::nullopt;
    }

    if (crc32(payload) != header.crc32) {
        return std::nullopt;
    }

    return payload;
}

std::optional<std::pair<RecordHeader, std::string>> Segment::read_next(uint64_t& file_pos) {
    file_.seekg(static_cast<std::streamoff>(file_pos), std::ios::beg);

    RecordHeader header;
    file_.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (!file_ || file_.gcount() == 0) {
        return std::nullopt;
    }

    std::string payload;
    payload.resize(header.size);
    file_.read(payload.data(), header.size);
    if (!file_) {
        return std::nullopt;
    }

    if (crc32(payload) != header.crc32) {
        return std::nullopt;
    }

    uint64_t next_pos = file_pos + sizeof(header) + header.size;
    file_pos = next_pos;

    return std::make_optional(std::make_pair(header, std::move(payload)));
}

uint64_t Segment::size_bytes() const {
    auto status = std::filesystem::status(path_);
    auto size = std::filesystem::file_size(path_);
    (void)status;
    return static_cast<uint64_t>(size);
}

// Placeholder crc32, replace with a real one later
uint32_t crc32(const std::string& data) {
    uint32_t hash = 0u;
    for (unsigned char c : data) {
        hash = hash * 16777619u ^ c;
    }
    return hash;
}

}  // namespace nebula
