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
    uint64_t max_segment_bytes{8 * 1024 * 1024}; // 8MB default (tune later)
    uint64_t retention_max_bytes{0};     // 0 = disabled
    uint64_t retention_max_segments{0};  // 0 = disabled

};

class NebulaLog {
public:
    explicit NebulaLog(const LogConfig& cfg);
    ~NebulaLog();

    // Append a record, returns its logical offset (0 based)
    uint64_t append(const std::string& payload);

    // Read record at logical offset. Returns nullopt if out of range or corrupted
    std::optional<std::string> read(uint64_t logical_offset);

    // Delete old segments (safe) while keeping offsets >= min_offset_to_keep
    void cleanup(uint64_t min_offset_to_keep);

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
    struct EntryRef {
        uint32_t seg;     // segment index in segments_
        uint64_t pos;     // byte position within that segment file
    };
    
    struct Segment {
        uint64_t base_offset;
        std::filesystem::path path;
        std::fstream file;
        uint64_t size_bytes{0};
        uint64_t next_offset{0};  
    };
    
    LogConfig cfg_;
    std::vector<Segment> segments_;            // ordered by base_offset
    std::vector<EntryRef> offset_index_;       // logical_offset -> {segment, pos}
    uint64_t base_logical_offset_{0}; // first logical offset represented by offset_index_
    uint64_t next_logical_offset_{0};
    
    void recover_segments();
    void roll_segment();                       // create a new active segment at next_logical_offset_
    Segment& active_segment();
    static uint64_t file_size_bytes(const std::filesystem::path& p);
    static std::string segment_filename(const std::string& prefix, uint64_t base_offset);
    static bool parse_segment_base(const std::string& name, const std::string& prefix, uint64_t& out_base);
    
};

} // namespace nebula
