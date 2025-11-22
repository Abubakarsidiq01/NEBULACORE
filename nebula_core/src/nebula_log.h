#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>
#include <filesystem>
#include <fstream>

namespace nebula {

struct LogConfig {
    std::string directory;       // directory for log files
    std::string base_filename;   // base file name, e.g. "segment_00000000.log"
};

class NebulaLog {
public:
    explicit NebulaLog(const LogConfig& cfg);
    ~NebulaLog();

    // Append a record, returns its logical offset (0 based)
    uint64_t append(const std::string& payload);

    // Read record at logical offset. Returns nullopt if out of range or corrupted
    std::optional<std::string> read(uint64_t logical_offset);

    class Iterator {
    public:
        Iterator(NebulaLog* log, uint64_t start_offset);

        // Move to next record, returns false if no more
        bool next();

        // Logical offset of the current record (valid only after next() returned true)
        uint64_t offset() const;

        // Value of the current record (valid only after next() returned true)
        const std::string& value() const;

    private:
        NebulaLog* log_;
        uint64_t current_offset_;
        std::string current_value_;
        bool initialized_;
    };

    // Create an iterator that scans from start_offset forward
    Iterator scan(uint64_t start_offset);

private:
    void recover_index();

    std::filesystem::path path_;
    std::fstream file_;
    std::vector<uint64_t> offset_index_;   // logical_offset -> file position
    uint64_t next_logical_offset_{0};
};

} // namespace nebula
