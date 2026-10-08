#pragma once
#include "app_config.hpp"
#include <memory>
#include <stdexcept>
namespace asstats {
class ArchiveStorageError : public std::runtime_error {
public:
    ArchiveStorageError(const std::string& message,int code):std::runtime_error(message),code_(code){}
    int sqlite_code()const noexcept{return code_;}
private:
    int code_;
};
struct ArchiveJournalLimits {
    uint64_t database_bytes=268435456,minute_bytes=16777216;
    size_t minute_keys=100000,active_keys=1000000,outbox_rows=1000000,receipt_minutes=64;
};
struct ArchiveBatch {
    uint64_t id{};
    std::string archive;
    std::vector<WindowRow> rows;
};
// Single-owner disk API. The receiver must call it through a bounded disk queue.
class ArchiveJournal {
public:
    ArchiveJournal(const std::string& directory,const std::vector<ArchiveConfig>&,
                   uint64_t first_minute,ArchiveJournalLimits={});
    ~ArchiveJournal();
    ArchiveJournal(const ArchiveJournal&)=delete;
    ArchiveJournal& operator=(const ArchiveJournal&)=delete;
    // An empty full minute is coverage, not a zero point for every series.
    // Identical recent replay is a no-op; conflicting or pruned old replay fails.
    bool ingest(uint64_t minute,bool partial,std::vector<WindowRow>);
    std::optional<ArchiveBatch> pending(const std::string& archive,size_t max_rows) const;
    // Called only after an immutable batch has been durably published to spool.
    void acknowledge(uint64_t batch_id);
    void acknowledge(const ArchiveBatch& published_part);
    void stage(uint64_t minute,bool partial,const std::string& payload);
    bool process_one();
    void set_history_boundary(uint64_t cutoff);
    void prepare_bootstrap();
    // Explicitly abandon only unprocessed historical coverage; cursor advances by normal ingest.
    void abandon_history(uint64_t cutoff);
    void activate_live(uint64_t cutoff);
    void stage_live(uint64_t minute,bool partial,const std::string& payload);
    uint64_t next_minute() const;
    std::string diagnostics() const;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
void validate_archive_registry(const std::string& directory,const std::vector<ArchiveConfig>&);
std::string archive_payload(const std::vector<WindowRow>&,size_t max_bytes=SIZE_MAX);
std::vector<WindowRow> archive_rows(uint64_t minute,bool partial,const std::string&,size_t max_keys);
}
