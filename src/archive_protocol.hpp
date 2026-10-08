#pragma once
#include <cstdint>
namespace asstats {
struct ArchiveRequest {uint64_t minute{},size{};uint32_t partial{},operation{};};
struct ArchiveReply {uint64_t size{};uint32_t ok=1,reserved{};};
constexpr uint64_t archive_reply_limit=262144;
}
