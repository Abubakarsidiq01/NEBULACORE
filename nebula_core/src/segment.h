#pragma once

#include <cstdint>
#include <fstream>
#include <string>
#include <optional>

namespace nebula {

struct RecordHeader {
    uint32_t crc32;
    uint32_t size;
    uint64_t offset;
};

class Segment {
public:
    Segment(const std::string& path, bool create_new);

    // Append one record, returns its file position in bytes from start
    uint64_t append(uint64_t logical_offset, const std::string& payload);

    // Read record by scanning from given file position
    std::optional<std::string> read_at_file_pos(uint64_t file_pos);

    // Read next record starting at file_pos, update file_pos to next record
    std::optional<std::pair<RecordHeader, std::string>> read_next(uint64_t& file_pos);

    // Get size in bytes of the segment file
    uint64_t size_bytes() const;

private:
    std::string path_;
    std::fstream file_;
};

uint32_t crc32(const std::string& data);

}  // namespace nebula
