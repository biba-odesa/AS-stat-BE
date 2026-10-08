#pragma once
#include "app_config.hpp"
#include <memory>
namespace asstats {
// Owns independent bounded IPC queues and isolated state workers.
class ArchiveManager {
public:
    ArchiveManager(const AppConfig&,const std::string& config_path,uint64_t start_minute);
    ~ArchiveManager();
    bool submit(uint64_t minute,bool partial,const std::vector<WindowRow>&);
    bool finish();
    std::string diagnostics()const;
private:
    struct Impl;std::unique_ptr<Impl> impl_;
};
}
