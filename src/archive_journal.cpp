#include "asstats/archive_journal.hpp"
#include <sqlite3.h>
#include <algorithm>
#include <filesystem>
#include <sstream>
#include <stdexcept>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
namespace asstats {
namespace {
struct Statement {
    sqlite3_stmt* p{};
    explicit Statement(sqlite3* db,const char* sql) {
        if(sqlite3_prepare_v2(db,sql,-1,&p,nullptr)!=SQLITE_OK)throw ArchiveStorageError(sqlite3_errmsg(db),sqlite3_extended_errcode(db));
    }
    ~Statement(){sqlite3_finalize(p);}
    void text(int n,const std::string& v){if(sqlite3_bind_text(p,n,v.data(),static_cast<int>(v.size()),SQLITE_TRANSIENT)!=SQLITE_OK)throw std::runtime_error("SQLite bind failed");}
    void number(int n,uint64_t v){if(v>INT64_MAX||sqlite3_bind_int64(p,n,static_cast<sqlite3_int64>(v))!=SQLITE_OK)throw std::runtime_error("SQLite integer out of range");}
    void reset(){sqlite3_reset(p);sqlite3_clear_bindings(p);}
    bool row(){int r=sqlite3_step(p);if(r==SQLITE_ROW)return true;if(r!=SQLITE_DONE)throw ArchiveStorageError(sqlite3_errmsg(sqlite3_db_handle(p)),sqlite3_extended_errcode(sqlite3_db_handle(p)));return false;}
    uint64_t integer(int n)const{return static_cast<uint64_t>(sqlite3_column_int64(p,n));}
    std::string string(int n)const{const auto* s=sqlite3_column_text(p,n);return s?reinterpret_cast<const char*>(s):"";}
};
void exec(sqlite3* db,const std::string& sql){char* error=nullptr;if(sqlite3_exec(db,sql.c_str(),nullptr,nullptr,&error)!=SQLITE_OK){std::string e=error?error:"SQLite failure";sqlite3_free(error);throw ArchiveStorageError(e,sqlite3_extended_errcode(db));}}
void rollback_noexcept(sqlite3* db) noexcept {
    // SQLITE_FULL can already abort the transaction. Never mask the original
    // storage error with a secondary "no transaction is active" error.
    if(!sqlite3_get_autocommit(db))sqlite3_exec(db,"ROLLBACK",nullptr,nullptr,nullptr);
}
uint64_t scalar(sqlite3* db,const char* sql){Statement q(db,sql);if(!q.row())throw std::runtime_error("Missing SQLite scalar");return q.integer(0);}
void bind_key(Statement& q,int n,const SeriesKey& key){q.text(n,key.link_id);q.number(n+1,key.asn);q.number(n+2,key.direction);q.number(n+3,key.ip_version);}
SeriesKey read_key(const Statement& q,int n){return {q.string(n),static_cast<uint32_t>(q.integer(n+1)),static_cast<uint8_t>(q.integer(n+2)),static_cast<uint8_t>(q.integer(n+3))};}
}
struct ArchiveJournal::Impl {
    sqlite3* db{};int lock=-1;ArchiveJournalLimits limits;uint64_t outbox_backpressure=0;
    ~Impl(){if(db)sqlite3_close(db);if(lock>=0)::close(lock);}
};
ArchiveJournal::ArchiveJournal(const std::string& directory,const std::vector<ArchiveConfig>& archives,uint64_t first,ArchiveJournalLimits limits):impl_(std::make_unique<Impl>()) {
    if(archives.empty()||archives.size()>16||first%60||!limits.minute_keys||!limits.minute_bytes||!limits.active_keys||!limits.outbox_rows||!limits.receipt_minutes||limits.receipt_minutes>10080||limits.database_bytes<1048576)throw std::invalid_argument("Invalid archive journal configuration");
    impl_->limits=limits;
    struct stat st{};
    if(lstat(directory.c_str(),&st)!=0||!S_ISDIR(st.st_mode)||st.st_uid!=geteuid()||(st.st_mode&0022))throw std::runtime_error("Archive state directory must exist, be owned by receiver and not be group/world writable");
    const auto lock_path=directory+"/.lock";
    impl_->lock=open(lock_path.c_str(),O_CREAT|O_RDWR|O_CLOEXEC|O_NOFOLLOW,0600);
    if(impl_->lock<0||flock(impl_->lock,LOCK_EX|LOCK_NB)!=0)throw std::runtime_error("Archive state is already locked or inaccessible");
    if(fstat(impl_->lock,&st)!=0||!S_ISREG(st.st_mode)||st.st_uid!=geteuid()||st.st_nlink!=1)throw std::runtime_error("Unsafe archive lock file");
    const auto path=directory+"/archives.sqlite";
    for(const auto* suffix:{"-journal","-wal","-shm"}) {
        if(lstat((path+suffix).c_str(),&st)==0&&(!S_ISREG(st.st_mode)||st.st_uid!=geteuid()||st.st_nlink!=1))throw std::runtime_error("Unsafe SQLite sidecar");
    }
    if(lstat(path.c_str(),&st)==0&&(!S_ISREG(st.st_mode)||st.st_uid!=geteuid()||st.st_nlink!=1))throw std::runtime_error("Unsafe archive database file");
    const int initial=open(path.c_str(),O_CREAT|O_RDWR|O_CLOEXEC|O_NOFOLLOW,0600);
    if(initial<0)throw std::runtime_error("Cannot create archive database");
    close(initial);
    if(sqlite3_open_v2(path.c_str(),&impl_->db,SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE,nullptr)!=SQLITE_OK)throw std::runtime_error("Cannot open archive state database");
    auto* db=impl_->db;
    sqlite3_create_function_v2(db,"asstat_u128_add",2,SQLITE_UTF8|SQLITE_DETERMINISTIC,nullptr,
        +[](sqlite3_context* context,int,sqlite3_value** values){
            try {
                const auto* a=sqlite3_value_text(values[0]);const auto* b=sqlite3_value_text(values[1]);
                if(!a||!b)throw std::runtime_error("Invalid stored uint128");
                auto sum=ByteCount::decimal(reinterpret_cast<const char*>(a));sum.add(ByteCount::decimal(reinterpret_cast<const char*>(b)));
                auto text=sum.str();sqlite3_result_text(context,text.c_str(),static_cast<int>(text.size()),SQLITE_TRANSIENT);
            }catch(const std::exception& e){sqlite3_result_error(context,e.what(),-1);}
        },nullptr,nullptr,nullptr);
    // DELETE journal + FULL makes the commit durable without an accumulating WAL.
    exec(db,"PRAGMA journal_mode=DELETE; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=1000;");
    const auto page=scalar(db,"PRAGMA page_size");
    exec(db,"PRAGMA max_page_count="+std::to_string(limits.database_bytes/page));
    if(scalar(db,"PRAGMA max_page_count")>limits.database_bytes/page)throw std::runtime_error("Existing archive database exceeds configured size limit");
    exec(db,"CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value INTEGER NOT NULL);"
            "CREATE TABLE IF NOT EXISTS archives(name TEXT PRIMARY KEY,url TEXT NOT NULL,interval INTEGER NOT NULL,start INTEGER NOT NULL,next INTEGER NOT NULL,partial INTEGER NOT NULL);"
            "CREATE TABLE IF NOT EXISTS inbox(minute INTEGER PRIMARY KEY,partial INTEGER NOT NULL,payload TEXT NOT NULL);"
            "CREATE TABLE IF NOT EXISTS skipped(archive TEXT,start INTEGER,end INTEGER,PRIMARY KEY(archive,start));"
            "CREATE TABLE IF NOT EXISTS gaps(start INTEGER PRIMARY KEY,end INTEGER NOT NULL);"
            "CREATE TABLE IF NOT EXISTS validation(batch INTEGER PRIMARY KEY,checked INTEGER NOT NULL);"
            "CREATE TABLE IF NOT EXISTS receipts(minute INTEGER PRIMARY KEY,canonical TEXT NOT NULL);"
            "CREATE TABLE IF NOT EXISTS totals(archive TEXT NOT NULL,link TEXT NOT NULL,asn INTEGER NOT NULL,direction INTEGER NOT NULL,family INTEGER NOT NULL,bytes TEXT NOT NULL,PRIMARY KEY(archive,link,asn,direction,family));"
            "CREATE TABLE IF NOT EXISTS batches(id INTEGER PRIMARY KEY AUTOINCREMENT,archive TEXT NOT NULL,start INTEGER NOT NULL,end INTEGER NOT NULL,partial INTEGER NOT NULL);"
            "CREATE TABLE IF NOT EXISTS output(batch INTEGER NOT NULL REFERENCES batches(id) ON DELETE CASCADE,link TEXT NOT NULL,asn INTEGER NOT NULL,direction INTEGER NOT NULL,family INTEGER NOT NULL,bytes TEXT NOT NULL,PRIMARY KEY(batch,link,asn,direction,family));");
    exec(db,"CREATE TRIGGER IF NOT EXISTS archive_validation_cleanup AFTER DELETE ON batches BEGIN DELETE FROM validation WHERE batch=old.id; END;");
    exec(db,"BEGIN IMMEDIATE");
    try {
        Statement version(db,"SELECT value FROM meta WHERE key='version'");
        if(version.row()){if(version.integer(0)!=1)throw std::runtime_error("Unsupported archive state schema");}
        else {exec(db,"INSERT INTO meta VALUES('version',1)");Statement n(db,"INSERT INTO meta VALUES('next',?)");n.number(1,first);n.row();}
        for(const auto& a:archives){
            if(!a.interval_seconds||a.interval_seconds%60||a.interval_seconds>86400)throw std::runtime_error("Invalid archive interval");
            Statement q(db,"SELECT url,interval FROM archives WHERE name=?");q.text(1,a.name);
            if(q.row()){if(q.string(0)!=a.url||q.integer(1)!=a.interval_seconds)throw std::runtime_error("Archive endpoint/interval conflicts with saved state: "+a.name);}
            else {
                if(scalar(db,"SELECT count(*) FROM receipts"))throw std::runtime_error("Adding an archive to processed state requires explicit bootstrap");
                Statement n(db,"INSERT INTO archives VALUES(?,?,?,?,?,?)");n.text(1,a.name);n.text(2,a.url);n.number(3,a.interval_seconds);n.number(4,first/a.interval_seconds*a.interval_seconds);n.number(5,first);n.number(6,first%a.interval_seconds!=0);n.row();}
        }
        if(scalar(db,"SELECT count(*) FROM archives")!=archives.size())throw std::runtime_error("Removing an archive with saved state is not supported");
        exec(db,"COMMIT");
    }catch(...){rollback_noexcept(db);throw;}
    const int directory_fd=open(directory.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW);
    if(directory_fd<0)throw std::runtime_error("Cannot open archive state directory for fsync");
    const int synced=fsync(directory_fd);close(directory_fd);
    if(synced)throw std::runtime_error("Cannot fsync archive state directory");
}
ArchiveJournal::~ArchiveJournal()=default;
uint64_t ArchiveJournal::next_minute() const{return scalar(impl_->db,"SELECT value FROM meta WHERE key='next'");}
bool ArchiveJournal::ingest(uint64_t minute,bool partial,std::vector<WindowRow> rows){
    if(minute%60||minute>INT64_MAX-86400||rows.size()>impl_->limits.minute_keys)throw std::runtime_error("Invalid/oversized minute batch");
    std::sort(rows.begin(),rows.end(),[](const auto& a,const auto& b){return a.key<b.key;});
    std::string canonical=partial?"partial\n":"full\n";
    const SeriesKey* previous=nullptr;
    for(const auto& r:rows){
        if(r.start!=minute||r.end!=minute+60||r.partial!=partial||r.key.link_id.empty()||r.key.link_id.size()>64||r.key.link_id.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")!=std::string::npos||r.key.direction>1||(r.key.ip_version!=4&&r.key.ip_version!=6))throw std::runtime_error("Invalid minute row");
        if(previous&&*previous==r.key)throw std::runtime_error("Duplicate key in global minute batch");
        previous=&r.key;
        // Length-prefix prevents distinct link names from colliding in the receipt.
        canonical+=std::to_string(r.key.link_id.size())+":"+r.key.link_id+":"+std::to_string(r.key.asn)+":"+std::to_string(r.key.direction)+":"+std::to_string(r.key.ip_version)+":"+r.bytes.str()+"\n";
    }
    auto* db=impl_->db;exec(db,"BEGIN IMMEDIATE");
    try {
        Statement old(db,"SELECT canonical FROM receipts WHERE minute=?");old.number(1,minute);
        if(old.row()){if(old.string(0)!=canonical)throw std::runtime_error("Conflicting replay of saved minute");exec(db,"ROLLBACK");return false;}
        if(minute!=next_minute())throw std::runtime_error("Archive minute is out of order; submit explicit partial/empty coverage for gaps");
        Statement receipt(db,"INSERT INTO receipts VALUES(?,?)");receipt.number(1,minute);receipt.text(2,canonical);receipt.row();
        Statement archives(db,"SELECT name,interval,start,partial FROM archives ORDER BY name");
        while(archives.row()){
            const auto name=archives.string(0);const auto interval=archives.integer(1),start=archives.integer(2);const bool incomplete=partial||archives.integer(3);
            // Partial contributions remain in state but are never delivered as full totals.
            Statement n(db,"INSERT INTO totals VALUES(?,?,?,?,?,?) ON CONFLICT(archive,link,asn,direction,family) DO UPDATE SET bytes=asstat_u128_add(bytes,excluded.bytes)");
            for(const auto& r:rows){n.reset();n.text(1,name);bind_key(n,2,r.key);n.text(6,r.bytes.str());n.row();}
            if(minute+60==start+interval){
                if(!incomplete){
                    Statement b(db,"INSERT INTO batches(archive,start,end,partial) VALUES(?,?,?,0)");b.text(1,name);b.number(2,start);b.number(3,start+interval);b.row();
                    const auto id=static_cast<uint64_t>(sqlite3_last_insert_rowid(db));
                    Statement copy(db,"INSERT INTO output SELECT ?,link,asn,direction,family,bytes FROM totals WHERE archive=?");copy.number(1,id);copy.text(2,name);copy.row();
                    // Empty coverage is retained by the cursor, not an empty delivery batch.
                    Statement empty(db,"DELETE FROM batches WHERE id=? AND NOT EXISTS(SELECT 1 FROM output WHERE batch=?)");empty.number(1,id);empty.number(2,id);empty.row();
                }else {
                    exec(db,"INSERT INTO meta VALUES('partial_windows',1) ON CONFLICT(key) DO UPDATE SET value=value+1");
                    Statement skipped(db,"INSERT INTO skipped VALUES(?,?,?)");skipped.text(1,name);skipped.number(2,start);skipped.number(3,start+interval);skipped.row();
                    Statement prune_skipped(db,"DELETE FROM skipped WHERE archive=? AND start NOT IN (SELECT start FROM skipped WHERE archive=? ORDER BY start DESC LIMIT 256)");prune_skipped.text(1,name);prune_skipped.text(2,name);prune_skipped.row();
                }
                Statement clear(db,"DELETE FROM totals WHERE archive=?");clear.text(1,name);clear.row();
            }
            Statement update(db,"UPDATE archives SET start=?,next=?,partial=? WHERE name=?");update.number(1,minute+60==start+interval?start+interval:start);update.number(2,minute+60);update.number(3,minute+60==start+interval?0:incomplete);update.text(4,name);update.row();
        }
        if(scalar(db,"SELECT count(*) FROM totals")>impl_->limits.active_keys||scalar(db,"SELECT count(*) FROM output")>impl_->limits.outbox_rows)throw std::runtime_error("Archive journal key/outbox limit reached");
        Statement prune(db,"DELETE FROM receipts WHERE minute NOT IN (SELECT minute FROM receipts ORDER BY minute DESC LIMIT ?)");prune.number(1,impl_->limits.receipt_minutes);prune.row();
        Statement expired_gaps(db,"DELETE FROM gaps WHERE end<=?");expired_gaps.number(1,minute+60);expired_gaps.row();
        Statement remove(db,"DELETE FROM inbox WHERE minute=?");remove.number(1,minute);remove.row();
        exec(db,"INSERT INTO meta VALUES('ingested_minutes',1) ON CONFLICT(key) DO UPDATE SET value=value+1");
        Statement cursor(db,"UPDATE meta SET value=? WHERE key='next'");cursor.number(1,minute+60);cursor.row();exec(db,"COMMIT");return true;
    }catch(...){rollback_noexcept(db);throw;}
}
std::optional<ArchiveBatch> ArchiveJournal::pending(const std::string& archive,size_t max_rows)const{
    auto* db=impl_->db;Statement q(db,"SELECT id,start,end,partial FROM batches WHERE archive=? AND (start>=coalesce((SELECT value FROM meta WHERE key='history_boundary'),0) OR EXISTS(SELECT 1 FROM validation WHERE batch=batches.id)) ORDER BY id LIMIT 1");q.text(1,archive);if(!q.row())return std::nullopt;
    ArchiveBatch batch{q.integer(0),archive,{}};Statement rows(db,"SELECT link,asn,direction,family,bytes FROM output WHERE batch=? ORDER BY link,asn,direction,family LIMIT ?");rows.number(1,batch.id);rows.number(2,max_rows);
    while(rows.row()){batch.rows.push_back({q.integer(1),q.integer(2),read_key(rows,0),ByteCount::decimal(rows.string(4)),q.integer(3)!=0});}return batch;
}
void ArchiveJournal::acknowledge(uint64_t id){Statement q(impl_->db,"DELETE FROM batches WHERE id=?");q.number(1,id);q.row();}
void ArchiveJournal::acknowledge(const ArchiveBatch& part) {
    auto* db=impl_->db;exec(db,"BEGIN IMMEDIATE");
    try {
        for(const auto& r:part.rows) {
            Statement q(db,"DELETE FROM output WHERE batch=? AND link=? AND asn=? AND direction=? AND family=? AND bytes=?");q.number(1,part.id);bind_key(q,2,r.key);q.text(6,r.bytes.str());q.row();
            if(sqlite3_changes(db)!=1)throw std::runtime_error("Archive handoff acknowledgement conflicts with saved output");
        }
        Statement empty(db,"DELETE FROM batches WHERE id=? AND NOT EXISTS(SELECT 1 FROM output WHERE batch=?)");empty.number(1,part.id);empty.number(2,part.id);empty.row();exec(db,"COMMIT");
    }catch(...){rollback_noexcept(db);throw;}
}
void validate_archive_registry(const std::string& directory,const std::vector<ArchiveConfig>& configs) {
    for(const auto& entry:std::filesystem::directory_iterator(directory)) {
        if(!entry.is_directory()||entry.is_symlink())continue;
        const auto file=entry.path()/"archives.sqlite";struct stat st{};
        if(lstat(file.c_str(),&st))continue;
        if(!S_ISREG(st.st_mode)||st.st_uid!=geteuid()||st.st_nlink!=1)throw std::runtime_error("Unsafe archive registry database");
        sqlite3* db=nullptr;
        if(sqlite3_open_v2(file.c_str(),&db,SQLITE_OPEN_READONLY,nullptr)!=SQLITE_OK){if(db)sqlite3_close(db);throw std::runtime_error("Cannot inspect saved archive registry");}
        // A live worker can briefly hold the DELETE-journal writer lock.
        sqlite3_busy_timeout(db,1000);
        try {
            {Statement q(db,"SELECT name,url,interval FROM archives");while(q.row())for(const auto& a:configs) {
                if((a.url==q.string(1)&&(a.name!=q.string(0)||a.interval_seconds!=q.integer(2)))||(a.name==q.string(0)&&(a.url!=q.string(1)||a.interval_seconds!=q.integer(2))))throw std::runtime_error("Archive endpoint/interval conflicts with saved state: "+a.name+"; endpoint belongs to another saved resolution");
            }}sqlite3_close(db);
        }catch(...){sqlite3_close(db);throw;}
    }
}
std::string archive_payload(const std::vector<WindowRow>& rows,size_t max_bytes) {
    std::string out;
    for(const auto& r:rows) {
        auto line=r.key.link_id+"\t"+std::to_string(r.key.asn)+"\t"+std::to_string(r.key.direction)+"\t"+std::to_string(r.key.ip_version)+"\t"+r.bytes.str()+"\n";
        if(line.size()>max_bytes-out.size())throw std::length_error("Minute serialization exceeds archive memory budget");
        out+=line;
    }
    return out;
}
std::vector<WindowRow> archive_rows(uint64_t minute,bool partial,const std::string& payload,size_t max_keys) {
    std::istringstream in(payload);std::vector<WindowRow> rows;std::string line;
    while(std::getline(in,line)) {
        if(rows.size()>=max_keys||line.size()>256)throw std::runtime_error("Archive input row limit/length exceeded");
        std::istringstream row(line);std::string link,bytes,extra;uint64_t asn=0,direction=0,family=0;
        if(!(row>>link>>asn>>direction>>family>>bytes)||(row>>extra)||asn>UINT32_MAX||direction>1||(family!=4&&family!=6))throw std::runtime_error("Invalid archive input payload");
        rows.push_back({minute,minute+60,{link,static_cast<uint32_t>(asn),static_cast<uint8_t>(direction),static_cast<uint8_t>(family)},ByteCount::decimal(bytes),partial});
    }
    return rows;
}
void ArchiveJournal::stage(uint64_t minute,bool partial,const std::string& payload) {
    if(minute%60||payload.size()>impl_->limits.minute_bytes)throw std::runtime_error("Invalid staged minute/size");
    auto* db=impl_->db;exec(db,"BEGIN IMMEDIATE");
    try {
        if(minute<next_minute())throw std::runtime_error("Staged minute precedes committed cursor");
        Statement old(db,"SELECT partial,payload FROM inbox WHERE minute=?");old.number(1,minute);
        if(old.row()){if(old.integer(0)!=partial||old.string(1)!=payload)throw std::runtime_error("Conflicting staged minute");}
        else {Statement n(db,"INSERT INTO inbox VALUES(?,?,?)");n.number(1,minute);n.number(2,partial);n.text(3,payload);n.row();}
        exec(db,"COMMIT");
    }catch(...){rollback_noexcept(db);throw;}
}
bool ArchiveJournal::process_one() {
    auto* db=impl_->db;const auto minute=next_minute();std::string payload;bool partial=false;
    {Statement q(db,"SELECT partial,payload FROM inbox WHERE minute=?");q.number(1,minute);if(!q.row()) {
        Statement gap(db,"SELECT 1 FROM gaps WHERE start<=? AND end>? LIMIT 1");gap.number(1,minute);gap.number(2,minute);if(!gap.row())return false;partial=true;
    }else {partial=q.integer(0)!=0;payload=q.string(1);}}
    auto rows=archive_rows(minute,partial,payload,impl_->limits.minute_keys);
    // Bounded outbox saturation is backpressure, not a failed transaction.
    // Leave this immutable input pending while the independent sender drains.
    if(!partial) {
        auto available=impl_->limits.outbox_rows-scalar(db,"SELECT count(*) FROM output");
        Statement closing(db,"SELECT name,start,interval,partial FROM archives");
        while(closing.row())if(!closing.integer(3)&&minute+60==closing.integer(1)+closing.integer(2)) {
            Statement count(db,"SELECT count(*) FROM totals WHERE archive=?");count.text(1,closing.string(0));count.row();auto needed=count.integer(0);
            if(needed>available){++impl_->outbox_backpressure;return false;}
            Statement exists(db,"SELECT 1 FROM totals WHERE archive=? AND link=? AND asn=? AND direction=? AND family=?");
            for(const auto& row:rows){exists.reset();exists.text(1,closing.string(0));bind_key(exists,2,row.key);if(!exists.row()&&++needed>available){++impl_->outbox_backpressure;return false;}}
            available-=needed;
        }
    }
    return ingest(minute,partial,std::move(rows));
}
void ArchiveJournal::set_history_boundary(uint64_t cutoff) {
    if(cutoff%60)throw std::runtime_error("History boundary must be minute aligned");
    Statement old(impl_->db,"SELECT value FROM meta WHERE key='history_boundary'");
    if(old.row()){if(old.integer(0)!=cutoff)throw std::runtime_error("History boundary is immutable");return;}
    Statement q(impl_->db,"INSERT INTO meta VALUES('history_boundary',?)");q.number(1,cutoff);q.row();
}
void ArchiveJournal::abandon_history(uint64_t cutoff) {
    if(cutoff%60||cutoff>INT64_MAX-86400)throw std::runtime_error("Invalid abandonment boundary");
    auto* db=impl_->db;exec(db,"BEGIN IMMEDIATE");
    try {
        Statement boundary(db,"SELECT value FROM meta WHERE key='history_boundary'");
        if(!boundary.row()||boundary.integer(0)!=cutoff)throw std::runtime_error("Abandonment must use the saved immutable history boundary");
        const auto cursor=next_minute();
        Statement old(db,"SELECT value FROM meta WHERE key='history_abandoned'");
        if(old.row()){if(old.integer(0)!=cutoff)throw std::runtime_error("Conflicting abandonment");exec(db,"COMMIT");return;}
        if(cursor>cutoff)throw std::runtime_error("Archive has already processed live coverage");
        Statement pending(db,"SELECT count(*) FROM inbox WHERE minute<?");pending.number(1,cutoff);pending.row();
        if(pending.integer(0))throw std::runtime_error("Historical inbox is not empty; preserve and inspect before abandonment");
        exec(db,"CREATE TABLE IF NOT EXISTS administrative_gaps(start INTEGER PRIMARY KEY,end INTEGER NOT NULL,reason TEXT NOT NULL)");
        if(cursor<cutoff){
            Statement gap(db,"INSERT INTO gaps VALUES(?,?) ON CONFLICT(start) DO UPDATE SET end=max(end,excluded.end)");gap.number(1,cursor);gap.number(2,cutoff);gap.row();
            Statement audit(db,"INSERT INTO administrative_gaps VALUES(?,?,'historical_backfill_cancelled')");audit.number(1,cursor);audit.number(2,cutoff);audit.row();
        }
        Statement flag(db,"INSERT INTO meta VALUES('history_abandoned',?)");flag.number(1,cutoff);flag.row();exec(db,"COMMIT");
    }catch(...){rollback_noexcept(db);throw;}
}
void ArchiveJournal::prepare_bootstrap() {
    auto* db=impl_->db;
    if(scalar(db,"SELECT count(*) FROM receipts")||scalar(db,"SELECT count(*) FROM inbox"))throw std::runtime_error("Bootstrap must initialize a new archive; existing state is never reset");
    exec(db,"BEGIN IMMEDIATE");
    try {exec(db,"INSERT INTO meta VALUES('bootstrap_pending',1); INSERT INTO meta VALUES('history_boundary',9223372036854775800);");exec(db,"COMMIT");}
    catch(...){rollback_noexcept(db);throw;}
}
void ArchiveJournal::activate_live(uint64_t cutoff) {
    auto* db=impl_->db;exec(db,"BEGIN IMMEDIATE");
    try {
        Statement bootstrap(db,"SELECT value FROM meta WHERE key='bootstrap_pending'");
        if(bootstrap.row()&&bootstrap.integer(0)==1) {
            Statement q(db,"UPDATE meta SET value=? WHERE key='history_boundary'");q.number(1,cutoff);q.row();exec(db,"UPDATE meta SET value=0 WHERE key='bootstrap_pending'");
        }else {
            Statement q(db,"INSERT INTO meta VALUES('history_boundary',?) ON CONFLICT(key) DO NOTHING");q.number(1,cutoff);q.row();
        }
        exec(db,"COMMIT");
    }catch(...){rollback_noexcept(db);throw;}
}
void ArchiveJournal::stage_live(uint64_t minute,bool partial,const std::string& payload) {
    if(minute%60||minute>INT64_MAX-86400||payload.size()>impl_->limits.minute_bytes)throw std::runtime_error("Invalid live minute/size");
    auto* db=impl_->db;
    if(minute<next_minute()) {
        if(!partial)throw std::runtime_error("Live replay precedes committed cursor");
        exec(db,"INSERT INTO meta VALUES('ignored_restart_partial',1) ON CONFLICT(key) DO UPDATE SET value=value+1");return;
    }
    exec(db,"BEGIN IMMEDIATE");
    try {
        uint64_t lower=std::max(next_minute(),scalar(db,"SELECT value FROM meta WHERE key='history_boundary'"));
        Statement last(db,"SELECT value FROM meta WHERE key='last_live_minute'");if(last.row())lower=std::max(lower,last.integer(0)+60);
        if(minute>lower){Statement gap(db,"INSERT INTO gaps VALUES(?,?)");gap.number(1,lower);gap.number(2,minute);gap.row();}
        Statement old(db,"SELECT partial,payload FROM inbox WHERE minute=?");old.number(1,minute);
        if(old.row()){
            // A graceful restart can produce two different partial fragments
            // of the same minute. Keep the first immutable partial receipt;
            // neither fragment may become a full archive window.
            if(partial&&old.integer(0)){
                exec(db,"INSERT INTO meta VALUES('ignored_restart_partial',1) ON CONFLICT(key) DO UPDATE SET value=value+1");
            }else if(old.integer(0)!=partial||old.string(1)!=payload)throw std::runtime_error("Conflicting live inbox minute");
        }
        else {Statement input(db,"INSERT INTO inbox VALUES(?,?,?)");input.number(1,minute);input.number(2,partial);input.text(3,payload);input.row();}
        Statement cursor(db,"INSERT INTO meta VALUES('last_live_minute',?) ON CONFLICT(key) DO UPDATE SET value=max(value,excluded.value)");cursor.number(1,minute);cursor.row();exec(db,"COMMIT");
    }catch(...){rollback_noexcept(db);throw;}
}
std::string ArchiveJournal::diagnostics()const{
    auto* db=impl_->db;std::ostringstream o;o<<"{\"database_limit_bytes\":"<<scalar(db,"PRAGMA max_page_count")*scalar(db,"PRAGMA page_size")<<",\"database_file_bytes\":"<<scalar(db,"PRAGMA page_count")*scalar(db,"PRAGMA page_size")<<",\"free_page_bytes\":"<<scalar(db,"PRAGMA freelist_count")*scalar(db,"PRAGMA page_size")<<",\"next_minute\":"<<next_minute()<<",\"outbox_backpressure\":"<<impl_->outbox_backpressure<<",\"ingested_minutes\":"<<scalar(db,"SELECT coalesce((SELECT value FROM meta WHERE key='ingested_minutes'),0)")<<",\"partial_windows\":"<<scalar(db,"SELECT coalesce((SELECT value FROM meta WHERE key='partial_windows'),0)")<<",\"staged_minutes\":"<<scalar(db,"SELECT count(*) FROM inbox")<<",\"active_keys\":"<<scalar(db,"SELECT count(*) FROM totals")<<",\"outbox_rows\":"<<scalar(db,"SELECT count(*) FROM output")<<",\"pending_batches\":"<<scalar(db,"SELECT count(*) FROM batches")<<",\"receipt_minutes\":"<<scalar(db,"SELECT count(*) FROM receipts")<<'}';return o.str();
}
}
