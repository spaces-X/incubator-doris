#pragma once
#include <filesystem>
#include <fstream>
#include <iostream>

#include "olap/rowset/rowset_writer.h"
#include "common/status.h"


namespace doris {
class RowsetWriter;

class RowsetWriterCompaction {
public:
    RowsetWriterCompaction(std::unique_ptr<RowsetWriter> rowset_writer, std::string rowset_meta_path)
            : _rowset_writer(std::move(rowset_writer)), _rowset_meta_path(rowset_meta_path) {};
    Status init_rowset_writer_by_metas();
    Status doSegCompacton();





private:
    void _init_meta_files();
    std::unique_ptr<RowsetWriter> _rowset_writer;
    std::string _rowset_meta_path;
    std::vector<std::filesystem::directory_entry> _meta_files;
};

}
