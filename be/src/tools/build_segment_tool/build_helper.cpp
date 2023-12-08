#include "tools/build_segment_tool/build_helper.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/status.h"
#include "olap/file_header.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema_cache.h"
#include "runtime/exec_env.h"
#include "tools/build_segment_tool/builder_scanner_memtable.h"
#include "tools/build_segment_tool/rowset_writer_compaction.h"
#include "util/disk_info.h"
#include "util/mem_info.h"

namespace doris {

RowsetId segment_builder_default_rid = {1,1,1,1};
BuildHelper* BuildHelper::_s_instance = nullptr;

BuildHelper* BuildHelper::init_instance() {
    // DCHECK(_s_instance == nullptr);
    static BuildHelper instance;
    _s_instance = &instance;
    return _s_instance;
}

void BuildHelper::initial_build_env() {
    char doris_home[] = "DORIS_HOME=/tmp";
    putenv(doris_home);

    if (!doris::config::init(nullptr, true, false, true)) {
        LOG(FATAL) << "init config fail";
        exit(-1);
    }
    CpuInfo::init();
    DiskInfo::init();
    MemInfo::init();
    // write buffer size before flush
    config::write_buffer_size = 209715200;
    // max buffer size used in memtable for the aggregated table
    config::write_buffer_size_for_agg = 8194304000;
    // CONF_mInt64(memtable_max_buffer_size, "8194304000");
    config::enable_segcompaction = true;

    // std::shared_ptr<doris::MemTrackerLimiter> process_mem_tracker =
    //         std::make_shared<doris::MemTrackerLimiter>(MemTrackerLimiter::Type::GLOBAL,
    //         "Process");
    // doris::ExecEnv::GetInstance()->set_orphan_mem_tracker(process_mem_tracker);
    // doris::thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(process_mem_tracker,
    //                                                                         TUniqueId());

    // doris::thread_context()->thread_mem_tracker_mgr->init();
    // doris::thread_context()->thread_mem_tracker_mgr->set_check_limit(false);
    doris::TabletSchemaCache::create_global_schema_cache();
    doris::ChunkAllocator::init_instance(4096);
}

void BuildHelper::open(const std::string& meta_file, const std::string& build_dir,
                       const std::string& data_path, const std::string& file_type) {
    _meta_file = meta_file;
    _build_dir = build_dir;
    if (data_path.at(data_path.size() - 1) != '/') {
        _data_path = data_path + "/";
    } else {
        _data_path = data_path;
    }

    _file_type = file_type;

    std::filesystem::path dir_path(std::filesystem::absolute(std::filesystem::path(build_dir)));
    if (!std::filesystem::is_directory(std::filesystem::status(dir_path))) {
        LOG(FATAL) << "build dir should be a directory";
    }

    // init and open storage engine
    std::vector<doris::StorePath> paths;
    auto olap_res = doris::parse_conf_store_paths(_build_dir, &paths);
    if (!olap_res) {
        LOG(FATAL) << "parse config storage path failed, path=" << doris::config::storage_root_path;
        exit(-1);
    }
    auto exec_env = doris::ExecEnv::GetInstance();
    doris::ExecEnv::init(exec_env, paths);
    // doris::TabletSchemaCache::create_global_schema_cache();
    doris::EngineOptions options;
    options.store_paths = paths;
    options.backend_uid = doris::UniqueId::gen_uid();
    doris::StorageEngine* engine = nullptr;
    auto st = doris::StorageEngine::open(options, &engine);
    if (!st.ok()) {
        LOG(FATAL) << "fail to open StorageEngine, res=" << st;
        exit(-1);
    }
    exec_env->set_storage_engine(engine);
    EXIT_IF_ERROR(engine->start_bg_threads());
}

Status BuildHelper::seg_compaction() {
    TabletSharedPtr new_tablet = nullptr;
    Status status = _init_tablet(new_tablet);
    if (!status.ok()) {
        LOG(FATAL) << "fail to init tablet to storage :" << status.to_string();
        return status;
    }
//    TabletManager* tablet_mgr = StorageEngine::instance()->tablet_manager();
//    TabletSharedPtr tablet = tablet_mgr->get_tablet(new_tablet->tablet_id());
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_lo(1);
    RowsetIdUnorderedSet rowset_ids;
    DeleteBitmapPtr delete_bitmap = nullptr;
    int cur_max_version = 0;
    if (new_tablet->enable_unique_key_merge_on_write()) {
        if (delete_bitmap == nullptr) {
            delete_bitmap.reset(new DeleteBitmap(new_tablet->tablet_id()));
        }
        std::lock_guard<std::shared_mutex> lck(new_tablet->get_header_lock());
        cur_max_version = new_tablet->max_version_unlocked().second;
        rowset_ids = new_tablet->all_rs_id(cur_max_version);
    }
    std::unique_ptr<RowsetWriter> rowset_writer = nullptr;
    RowsetWriterContext context;
    context.txn_id = 1;
    context.load_id = load_id;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    context.tablet_schema = new_tablet->tablet_schema();
    context.newest_write_timestamp = UnixSeconds();
    context.tablet_id = new_tablet->table_id();
    context.tablet = new_tablet;
    context.is_direct_write = true;
    context.mow_context =
            std::make_shared<MowContext>(cur_max_version, rowset_ids, delete_bitmap);
    context.rowset_id = segment_builder_default_rid;
    new_tablet->create_rowset_writer(context, &rowset_writer);

    rowset_writer->set_writer_path(_build_dir + "/segment");
    RowsetWriterCompaction rwc(std::move(rowset_writer), _build_dir + "/rowset_meta_dir");
    RETURN_IF_ERROR(rwc.init_rowset_writer_by_metas());
    RETURN_IF_ERROR(rwc.doSegCompacton());
    return Status::OK();
}

Status BuildHelper::_init_tablet(TabletSharedPtr& input) {
    // load meta file
    io::FileReaderSPtr file_reader;
    TabletMeta* tablet_meta = new TabletMeta();
    if (_meta_file.ends_with(".json")) {
        // json type: for initing meta from json file
        std::string json_str;
        std::filesystem::path meta_file_path(_meta_file);
        RETURN_IF_ERROR(read_file_to_string(io::global_local_filesystem(), meta_file_path, &json_str));
        RETURN_IF_ERROR(tablet_meta->create_from_json(json_str));
    } else {
        // default: for initing meta from PB
        RETURN_IF_ERROR(tablet_meta->create_from_file(_meta_file));
    }
    //    if (!status.ok()) {
    //        std::cout << "load pb meta file:" << _meta_file << " failed"
    //                  << ", status:" << status << std::endl;
    //        return status;
    //    }

    LOG(INFO) << "table id:" << tablet_meta->table_id() << " tablet id:" << tablet_meta->tablet_id()
              << " shard id:" << tablet_meta->shard_id();

    // for debug
    // std::string json_meta;
    // json2pb::Pb2JsonOptions json_options;
    // json_options.pretty_json = true;
    // json_options.bytes_to_base64 = false;
    // tablet_meta->to_json(&json_meta, json_options);
    // LOG(INFO) << "\n" << json_meta;
    //
    auto data_dir = StorageEngine::instance()->get_store(_build_dir);
    TabletMetaSharedPtr tablet_meta_ptr(tablet_meta);
    TabletSharedPtr new_tablet = doris::Tablet::create_tablet_from_meta(tablet_meta_ptr, data_dir);
    Status status = StorageEngine::instance()->tablet_manager()->add_tablet_unlocked(
            tablet_meta->tablet_id(), new_tablet, false, true);
    input = std::move(new_tablet);
    return status;
}

Status BuildHelper::build() {
    TabletSharedPtr new_tablet = nullptr;

    Status status = _init_tablet(new_tablet);
    if (!status.ok()) {
        LOG(FATAL) << "fail to init tablet to storage :" << status.to_string();
        return status;
    }

    std::vector<std::filesystem::directory_entry> files;
    for (const auto& file : std::filesystem::directory_iterator(_data_path)) {
        auto file_path = file.path().string();
        if (file_path.substr(file_path.size() - _file_type.size()) == _file_type) {
            LOG(INFO) << "get file:" << file_path;
            files.emplace_back(file);
        }
    }

    BuilderScannerMemtable scanner(new_tablet, _build_dir, _file_type);
    scanner.doSegmentBuild(files);

    TabletMetaSharedPtr new_tablet_meta = std::make_shared<TabletMeta>();
    new_tablet->generate_tablet_meta_copy(new_tablet_meta);
    auto rowsets = new_tablet_meta->all_rs_metas();
    if (rowsets.size() != 1) {
        LOG(FATAL) << "rowset num from meta must be 1";
    }
    std::string local_rowset_meta = _build_dir + "/segment/rowset_meta.json";
    auto st = rowsets[0]->save_json_file(local_rowset_meta);
    if (!st.ok()) {
        int reTry = 3;
        LOG(WARNING) << " save rowset meta file error..., need retry...";
        while(--reTry >=0 && !st.ok()) {
            std::filesystem::remove(local_rowset_meta);
            st = rowsets[0]->save_json_file(local_rowset_meta);
        }
        if (reTry < 0) {
            LOG(WARNING) << " save rowset meta fail...";
            std::exit(1);
        }
    }

    std::string local_new_header = _build_dir + "/tablet_meta/" + std::to_string(new_tablet->tablet_id()) + ".hdr";
    std::filesystem::create_directory(_build_dir + "/tablet_meta/");

    st = new_tablet_meta->save(local_new_header);
    // post check
    std::filesystem::path header = local_new_header;
    if (!st.ok()) {
        LOG(WARNING) << " save tablet meta file error..., need retry...";
        std::filesystem::remove(header);
        int reTry = 3;
        while (--reTry >= 0 && !st.ok()) {
            st = new_tablet_meta->save(local_new_header);
        }

        if (reTry < 0) {
            LOG(WARNING) << " save tablet meta fail...";
            std::exit(1);
        }
    }
    size_t header_size = std::filesystem::file_size(header);
    if (header_size <= 0) {
        LOG(WARNING) << " header size abnormal ..." << local_new_header;
        std::exit(1);
    }
    LOG(INFO) << " got header size:" << header_size;
    return Status::OK();
}

Status BuildHelper::close() {
    doris::StorageEngine* engine = doris::ExecEnv::GetInstance()->storage_engine();
    if (engine == nullptr) {
        LOG(FATAL) << "storage engine is null" ;
    }
    RETURN_IF_ERROR(engine->stop_bg_threads());
    engine->stop();
    return Status::OK();
}

BuildHelper::~BuildHelper() {
    // doris::TabletSchemaCache::stop_and_join();
    doris::ExecEnv::destroy(ExecEnv::GetInstance());
}

} // namespace doris
