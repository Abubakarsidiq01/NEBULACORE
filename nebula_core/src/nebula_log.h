#pragma once

#include <cstdint>
#include <string>
#include <memory>
#include <optional>

namespace nebula {

struct LogConfig {
    std::string directory;       // directory for log files
    std::string base_filename;   // for example "segment_00000000.log"
};

class NebulaLog {
public:
    explicit NebulaLog(const LogConfig& cfg);
    ~NebulaLog();

    // Append a record and return its logical offset
    uint64_t append(const std::string& payload);

    // Read record at logical offset, returns std::nullopt if not found
    std::optional<std::string> read(uint64_t logical_offset);

    // Simple forward iterator from a starting offset
    class Iterator {
    public:
        Iterator(NebulaLog* log, uint64_t start_offset);

        bool next();                       // move to next record, returns false at end
        uint64_t offset() const;           // current logical offset
        const std::string& value() const;  // current payload

    private:
        NebulaLog* log_;
        uint64_t current_offset_;
        std::string current_value_;
        bool initialized_;
    };

    Iterator scan(uint64_t start_offset);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace nebula
