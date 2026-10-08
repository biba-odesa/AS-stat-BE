#include <sqlite3.h>
#include <source_location>
#include "asstats/archive_journal.hpp"
#include <filesystem>
#include <iostream>
#include <unistd.h>
#include <sys/wait.h>
using namespace asstats;
namespace {
void require(bool b,std::source_location location=std::source_location::current()){if(!b)throw std::runtime_error("Archive journal assertion failed at line "+std::to_string(location.line()));}
template<class F> void rejects(F f){bool failed=false;try{f();}catch(const std::exception&){failed=true;}require(failed);}
std::vector<ArchiveConfig> configs(){return {{"a","http://127.0.0.1:18001",300,7,1},{"b","http://127.0.0.1:18002",1800,31,1},{"c","http://127.0.0.1:18003",7200,370,1},{"custom","http://127.0.0.1:18004",420,8,1}};}
std::vector<WindowRow> rows(uint64_t m,bool partial=false){return {{m,m+60,{"link-a",0,0,4},ByteCount(100),partial},{m,m+60,{"link-b",4200000000U,1,6},ByteCount(200),partial}};}
}
int main(){
    char name[]="/tmp/asstat-journal-XXXXXX";const char* dir=mkdtemp(name);if(!dir)return 1;
    try {
        require(ByteCount::decimal(ByteCount::maximum().str())==ByteCount::maximum());
        rejects([]{ByteCount::decimal(ByteCount::maximum().str()+"0");});
        {
            ArchiveJournal db(dir,configs(),0);
            rejects([&]{ArchiveJournal other(dir,configs(),0);});
            for(uint64_t m=0;m<900;m+=60)require(db.ingest(m,false,rows(m)));
            require(!db.ingest(0,false,rows(0)));
            auto altered=rows(0);altered[0].bytes=ByteCount(101);rejects([&]{db.ingest(0,false,altered);});
            require(db.next_minute()==900);
            auto a=db.pending("a",100);require(a&&a->rows.size()==2&&a->rows[0].bytes==ByteCount(500));
            require(!db.pending("b",100));
            auto custom=db.pending("custom",100);require(custom&&custom->rows[0].bytes==ByteCount(700));
        }
        {
            // Restart halfway through a 30-minute and a two-hour window.
            ArchiveJournal db(dir,configs(),0);
            for(uint64_t m=900;m<7200;m+=60)db.ingest(m,false,rows(m));
            auto b=db.pending("b",100),c=db.pending("c",100);
            require(b&&b->rows[0].bytes==ByteCount(3000));require(c&&c->rows[0].bytes==ByteCount(12000));
            require(c->rows[1].bytes==ByteCount(24000));
            auto part=db.pending("c",1);require(part&&part->rows.size()==1);
            db.acknowledge(*part);auto rest=db.pending("c",1);require(rest&&rest->rows.size()==1&&rest->rows[0].key==c->rows[1].key);
            rejects([&]{db.acknowledge(*part);});
            sqlite3* verification=nullptr;
            require(sqlite3_open((std::string(dir)+"/archives.sqlite").c_str(),&verification)==SQLITE_OK);
            const auto marker="INSERT INTO validation VALUES("+std::to_string(rest->id)+",1)";
            require(sqlite3_exec(verification,marker.c_str(),nullptr,nullptr,nullptr)==SQLITE_OK);
            db.acknowledge(*rest);require(!db.pending("c",100));
            sqlite3_stmt* query=nullptr;require(sqlite3_prepare_v2(verification,"SELECT count(*) FROM validation",-1,&query,nullptr)==SQLITE_OK);
            require(sqlite3_step(query)==SQLITE_ROW&&sqlite3_column_int(query,0)==0);
            sqlite3_finalize(query);sqlite3_close(verification);
            rejects([&]{db.ingest(7260,false,rows(7260));});require(db.next_minute()==7200);
        }
        auto changed=configs();changed[0].interval_seconds=600;rejects([&]{ArchiveJournal db(dir,changed,0);});
        std::filesystem::remove_all(dir);std::filesystem::create_directory(dir);std::filesystem::permissions(dir,std::filesystem::perms::owner_all);
        {
            ArchiveJournal db(dir,configs(),60);
            for(uint64_t m=60;m<300;m+=60)db.ingest(m,false,rows(m));
            require(!db.pending("a",100));
            for(uint64_t m=300;m<600;m+=60)db.ingest(m,m==360,rows(m,m==360));
            require(!db.pending("a",100));
            for(uint64_t m=600;m<900;m+=60)db.ingest(m,false,{});
            require(!db.pending("a",100));require(db.next_minute()==900);
        }
        std::filesystem::remove_all(dir);std::filesystem::create_directory(dir);std::filesystem::permissions(dir,std::filesystem::perms::owner_all);
        {
            ArchiveJournalLimits limits;limits.active_keys=1;
            ArchiveJournal db(dir,configs(),0,limits);
            rejects([&]{db.ingest(0,false,rows(0));});require(db.next_minute()==0);
            require(db.ingest(0,false,{}));
        }
        std::filesystem::remove_all(dir);std::filesystem::create_directory(dir);std::filesystem::permissions(dir,std::filesystem::perms::owner_all);
        {
            ArchiveJournalLimits limits;limits.receipt_minutes=2;
            ArchiveJournal db(dir,configs(),0,limits);
            for(uint64_t m=0;m<180;m+=60)db.ingest(m,false,{});
            rejects([&]{db.ingest(0,false,{});});
            require(!db.ingest(120,false,{}));
            auto huge=rows(180);huge[0].bytes=ByteCount::maximum();db.ingest(180,false,huge);
            rejects([&]{db.ingest(240,false,rows(240));});require(db.next_minute()==240);
            require(db.ingest(240,false,{}));
        }
        std::filesystem::remove_all(dir);std::filesystem::create_directory(dir);std::filesystem::permissions(dir,std::filesystem::perms::owner_all);
        {
            // A live backlog must report SQLITE_FULL without advancing cursor
            // or committing a partial inbox row; a larger measured budget can
            // reopen exactly the same state and resume without bootstrap.
            std::vector<WindowRow> many;
            for(uint32_t asn=0;asn<10000;++asn)many.push_back({0,60,{"link-a",asn,0,4},ByteCount(100),false});
            const auto payload=archive_payload(many);uint64_t failed_minute=0;bool full=false;
            ArchiveJournalLimits small;small.database_bytes=1048576;
            {
                ArchiveJournal db(dir,configs(),0,small);
                for(uint64_t minute=0;minute<6000;minute+=60){
                    try{db.stage(minute,false,payload);}
                    catch(const ArchiveStorageError& e){require((e.sqlite_code()&255)==SQLITE_FULL);failed_minute=minute;full=true;break;}
                }
                require(full&&failed_minute>0&&db.next_minute()==0);
            }
            auto larger=small;larger.database_bytes=4194304;
            ArchiveJournal resumed(dir,configs(),0,larger);
            resumed.stage(failed_minute,false,payload);
            require(resumed.process_one());require(resumed.next_minute()==60);
        }
        std::filesystem::remove_all(dir);std::filesystem::create_directory(dir);std::filesystem::permissions(dir,std::filesystem::perms::owner_all);
        {
            ArchiveJournal db(dir,configs(),0);
            db.set_history_boundary(0);
            db.stage_live(0,true,archive_payload(rows(0,true)));
            auto fragment=rows(0,true);fragment[0].bytes=ByteCount(999);
            db.stage_live(0,true,archive_payload(fragment));
            require(db.process_one());require(db.next_minute()==60);
            rejects([&]{db.stage_live(60,false,archive_payload(rows(60)));db.stage_live(60,false,archive_payload(fragment));});
        }
        std::filesystem::remove_all(dir);std::filesystem::create_directory(dir);std::filesystem::permissions(dir,std::filesystem::perms::owner_all);
        {
            ArchiveJournal db(dir,configs(),0);db.set_history_boundary(600);
            db.stage(0,false,archive_payload(rows(0)));rejects([&]{db.abandon_history(600);});
            require(db.process_one());db.stage_live(600,false,archive_payload(rows(600)));
            rejects([&]{db.abandon_history(660);});db.abandon_history(600);db.abandon_history(600);
            require(db.next_minute()==60);
            while(db.next_minute()<660)require(db.process_one());
            require(!db.pending("a",100));
            for(uint64_t m=660;m<900;m+=60)db.ingest(m,false,rows(m));
            auto a=db.pending("a",100);require(a&&a->rows[0].start==600&&a->rows[0].bytes==ByteCount(500));
            require(!db.pending("b",100));
        }
        std::filesystem::remove_all(dir);std::filesystem::create_directory(dir);std::filesystem::permissions(dir,std::filesystem::perms::owner_all);
        {
            ArchiveJournalLimits limits;limits.outbox_rows=2;
            ArchiveJournal db(dir,{{"a","http://127.0.0.1:18001",60,7,1}},0,limits);
            db.stage(0,false,archive_payload(rows(0)));require(db.process_one());
            db.stage(60,false,archive_payload(rows(60)));require(!db.process_one());require(db.next_minute()==60);
            auto first=db.pending("a",100);require(first.has_value());db.acknowledge(*first);
            require(db.process_one());auto next=db.pending("a",100);require(next&&next->rows[0].start==60&&next->rows[0].bytes==ByteCount(100));
        }
        std::filesystem::remove_all(dir);std::filesystem::create_directory(dir);std::filesystem::permissions(dir,std::filesystem::perms::owner_all);
        const auto child=fork();if(child<0)throw std::runtime_error("fork failed");
        if(child==0){ArchiveJournal db(dir,configs(),0);db.ingest(0,false,rows(0));_exit(0);}
        int status=0;require(waitpid(child,&status,0)==child&&WIFEXITED(status)&&WEXITSTATUS(status)==0);
        {
            ArchiveJournal db(dir,configs(),0);require(db.next_minute()==60);require(!db.ingest(0,false,rows(0)));
            for(uint64_t m=60;m<300;m+=60)db.ingest(m,false,rows(m));
            auto a=db.pending("a",100);require(a&&a->rows[0].bytes==ByteCount(500));
        }
        {
            const auto child_dir=std::string(dir)+"/registry-child";std::filesystem::create_directory(child_dir);std::filesystem::permissions(child_dir,std::filesystem::perms::owner_all);
            {ArchiveJournal child_journal(child_dir,configs(),0);}
            int ready[2];require(pipe(ready)==0);const auto locker=fork();require(locker>=0);
            if(locker==0){
                close(ready[0]);sqlite3* connection=nullptr;
                if(sqlite3_open((child_dir+"/archives.sqlite").c_str(),&connection)!=SQLITE_OK)_exit(2);
                if(sqlite3_exec(connection,"BEGIN EXCLUSIVE",nullptr,nullptr,nullptr)!=SQLITE_OK)_exit(3);
                char byte=1;if(write(ready[1],&byte,1)!=1)_exit(4);usleep(100000);
                if(sqlite3_exec(connection,"COMMIT",nullptr,nullptr,nullptr)!=SQLITE_OK)_exit(5);
                sqlite3_close(connection);_exit(0);
            }
            close(ready[1]);char byte=0;require(read(ready[0],&byte,1)==1);close(ready[0]);
            validate_archive_registry(dir,configs());int child_status=0;require(waitpid(locker,&child_status,0)==locker&&WIFEXITED(child_status)&&WEXITSTATUS(child_status)==0);
        }
        std::filesystem::remove_all(dir);std::cout<<"archive journal tests passed\n";return 0;
    }catch(const std::exception& e){std::cerr<<e.what()<<'\n';std::filesystem::remove_all(dir);return 1;}
}
