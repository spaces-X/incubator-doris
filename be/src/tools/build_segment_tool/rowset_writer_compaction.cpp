#include "tools/build_segment_tool/rowset_writer_compaction.h"
#include "io/fs/fs_utils.h"

namespace doris {

Status RowsetWriterCompaction::init_rowset_writer_by_metas() {
    _init_meta_files();
    assert(_meta_files.size() < 100*1000); //
    // make sure the rowset meta file is sorted by name
    std::sort(_meta_files.begin(), _meta_files.end(),
              [](const std::filesystem::directory_entry& a, const std::filesystem::directory_entry& b) {
                  return a.path().string() < b.path().string();
              }
    );
    for (auto meta_file : _meta_files) {
        std::cout << "start to load rowset-meta file: " << meta_file.path().string() << std::endl;
        std::string json_str;
        RETURN_IF_ERROR(read_file_to_string(io::global_local_filesystem(), meta_file.path(), &json_str));
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->init_from_json(json_str);
        _rowset_writer->load_segments_from_meta(rowset_meta);
    }
    return Status::OK();
}

Status RowsetWriterCompaction::doSegCompacton() {
    RETURN_IF_ERROR(_rowset_writer->manual_segment_compaction());
    auto compacted_rowset = _rowset_writer->build();
    std::string compacted_meta_path = _rowset_meta_path + "/rowset_meta.json";
    RETURN_IF_ERROR(compacted_rowset->rowset_meta()->save_json_file(compacted_meta_path));
    return Status::OK();
}

void RowsetWriterCompaction::_init_meta_files() {
    if (_meta_files.size() > 0) {
        return;
    }
    for (const auto& file : std::filesystem::directory_iterator(_rowset_meta_path)) {
        auto file_path = file.path().string();
        _meta_files.emplace_back(file);
    }
}

}
