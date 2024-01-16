#include "tools/build_segment_tool/builder_scanner_memtable.h"

#include <gen_cpp/Exprs_types.h>

#include <cstddef>
#include <filesystem>
#include <ostream>
#include <utility>

#include "exec/tablet_info.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "olap/delta_writer.h"
#include "olap/olap_common.h"
#include "olap/olap_tuple.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/arrow/row_batch.h"
#include "util/mem_info.h"
#include "vec/exec/scan/new_file_scan_node.h"

namespace doris {

static const int TUPLE_ID_DST = 0;
static const int TUPLE_ID_SRC = 1;
static const int BATCH_SIZE = 8192;

BuilderScannerMemtable::BuilderScannerMemtable(TabletSharedPtr tablet, const std::string& build_dir,
                                               const std::string& file_type)
        : _runtime_state(TQueryGlobals()),
          _tablet(tablet),
          _build_dir(build_dir),
          _file_type(file_type) {
    auto* _exec_env = ExecEnv::GetInstance();
    _query_ctx = QueryContext::create_shared(1, _exec_env, TQueryOptions());
    // query_ctx->timeout_second = params.query_options.execution_timeout;
    init();
    TQueryOptions _options;
    _options.batch_size = BATCH_SIZE;
    _runtime_state.init(_query_ctx->query_id, _options, TQueryGlobals(), _exec_env);
    _runtime_state.set_query_ctx(_query_ctx.get());
    // _runtime_state.init_mem_trackers(uid);
}

void BuilderScannerMemtable::init() {
    TUniqueId uid;
    uid.hi = 1;
    uid.lo = 1;
    _query_ctx->query_id = uid;
//    int64_t bytes_limit = MemInfo::mem_limit();
    _query_ctx->query_mem_tracker = nullptr;
//    _query_ctx->query_mem_tracker = std::make_shared<MemTrackerLimiter>(
//            MemTrackerLimiter::Type::LOAD,
//            fmt::format("Load#Id={}", print_id(_query_ctx->query_id)), bytes_limit);
    // _query_ctx->query_mem_tracker->enable_print_log_usage();
    create_expr_info();
    init_desc_table();

    // Node Id
    _tnode.node_id = 0;
    _tnode.node_type = TPlanNodeType::FILE_SCAN_NODE;
    _tnode.num_children = 0;
    _tnode.limit = -1;
    _tnode.row_tuples.push_back(0);
    _tnode.nullable_tuples.push_back(false);
    _tnode.file_scan_node.tuple_id = 1;
    _tnode.__isset.file_scan_node = true;
}

TPrimitiveType::type BuilderScannerMemtable::getPrimitiveType(FieldType t) {
    switch (t) {
    case FieldType::OLAP_FIELD_TYPE_OBJECT: {
        return TPrimitiveType::OBJECT;
    }
    case FieldType::OLAP_FIELD_TYPE_HLL: {
        return TPrimitiveType::HLL;
    }
    case FieldType::OLAP_FIELD_TYPE_CHAR: {
        return TPrimitiveType::CHAR;
    }
    case FieldType::OLAP_FIELD_TYPE_VARCHAR: {
        return TPrimitiveType::VARCHAR;
    }
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        return TPrimitiveType::STRING;
    }
    case FieldType::OLAP_FIELD_TYPE_DATE: {
        return TPrimitiveType::DATE;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIME: {
        return TPrimitiveType::DATETIME;
    }
    case FieldType::OLAP_FIELD_TYPE_DATEV2: {
        return TPrimitiveType::DATEV2;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
        return TPrimitiveType::DATETIMEV2;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
        return TPrimitiveType::DECIMALV2;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32: {
        return TPrimitiveType::DECIMAL32;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64: {
        return TPrimitiveType::DECIMAL64;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: {
        return TPrimitiveType::DECIMAL128I;
    }
    case FieldType::OLAP_FIELD_TYPE_JSONB: {
        return TPrimitiveType::JSONB;
    }
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        return TPrimitiveType::BOOLEAN;
    }
    case FieldType::OLAP_FIELD_TYPE_TINYINT: {
        return TPrimitiveType::TINYINT;
    }
    case FieldType::OLAP_FIELD_TYPE_SMALLINT: {
        return TPrimitiveType::SMALLINT;
    }
    case FieldType::OLAP_FIELD_TYPE_INT: {
        return TPrimitiveType::INT;
    }
    case FieldType::OLAP_FIELD_TYPE_BIGINT: {
        return TPrimitiveType::BIGINT;
    }
    case FieldType::OLAP_FIELD_TYPE_LARGEINT: {
        return TPrimitiveType::LARGEINT;
    }
    case FieldType::OLAP_FIELD_TYPE_FLOAT: {
        return TPrimitiveType::FLOAT;
    }
    case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
        return TPrimitiveType::DOUBLE;
    }
    case FieldType::OLAP_FIELD_TYPE_ARRAY: {
        return TPrimitiveType::ARRAY;
    }
    default: {
        LOG(FATAL) << "unknown type error:" << int(t);
        exit(-1);
    }
    }
}

TDescriptorTable BuilderScannerMemtable::create_descriptor_tablet() {
    TDescriptorTableBuilder dtb;

    // build destination table descriptor 0
    {
        TTupleDescriptorBuilder tuple_builder;
        for (int i = 0; i < _tablet->num_columns(); i++) {
            const auto& col = _tablet->tablet_schema()->column(i);

            if (col.type() == FieldType::OLAP_FIELD_TYPE_DECIMAL ||
                col.type() == FieldType::OLAP_FIELD_TYPE_DECIMAL32 ||
                col.type() == FieldType::OLAP_FIELD_TYPE_DECIMAL64 ||
                col.type() == FieldType::OLAP_FIELD_TYPE_DECIMAL128I) {
                tuple_builder.add_slot(TSlotDescriptorBuilder()
                                               .decimal_type(col.precision(), col.frac())
                                               .nullable(col.is_nullable())
                                               .column_name(col.name())
                                               .column_pos(i)
                                               .build());
            } else if (col.type() == FieldType::OLAP_FIELD_TYPE_CHAR ||
                       col.type() == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
                tuple_builder.add_slot(TSlotDescriptorBuilder()
                                               .string_type(col.length())
                                               .nullable(col.is_nullable())
                                               .column_name(col.name())
                                               .column_pos(i)
                                               .build());
            } else {
                tuple_builder.add_slot(TSlotDescriptorBuilder()
                                               .type(thrift_to_type(getPrimitiveType(col.type())))
                                               .nullable(col.is_nullable())
                                               .column_name(col.name())
                                               .column_pos(i)
                                               .build());
            }
        }
        tuple_builder.build(&dtb);
    }

    // build source table descriptor 1
    {
        TTupleDescriptorBuilder tuple_builder;
        for (int i = 0; i < _tablet->num_columns(); i++) {
            const auto& col = _tablet->tablet_schema()->column(i);

            if (col.type() == FieldType::OLAP_FIELD_TYPE_DECIMAL ||
                col.type() == FieldType::OLAP_FIELD_TYPE_DECIMAL32 ||
                col.type() == FieldType::OLAP_FIELD_TYPE_DECIMAL64 ||
                col.type() == FieldType::OLAP_FIELD_TYPE_DECIMAL128I) {
                tuple_builder.add_slot(TSlotDescriptorBuilder()
                                               .decimal_type(col.precision(), col.frac())
                                               .column_name(col.name())
                                               .column_pos(i)
                                               .build());
            } else if (col.type() == FieldType::OLAP_FIELD_TYPE_CHAR ||
                       col.type() == FieldType::OLAP_FIELD_TYPE_VARCHAR) {
                tuple_builder.add_slot(TSlotDescriptorBuilder()
                                               .string_type(col.length())
                                               .nullable(col.is_nullable())
                                               .column_name(col.name())
                                               .column_pos(i)
                                               .build());
            } else {
                tuple_builder.add_slot(TSlotDescriptorBuilder()
                                               .type(thrift_to_type(getPrimitiveType(col.type())))
                                               .column_name(col.name())
                                               .column_pos(i)
                                               .build());
            }
        }
        tuple_builder.build(&dtb);
    }

    return dtb.desc_tbl();
}

void BuilderScannerMemtable::init_desc_table() {
    TDescriptorTable t_desc_table = create_descriptor_tablet();

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = _tablet->table_id();
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    t_table_desc.numCols = _tablet->num_columns();
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

    _runtime_state.set_desc_tbl(_desc_tbl);
    _query_ctx->desc_tbl = _desc_tbl;
}

void BuilderScannerMemtable::create_expr_info() {
    // TTypeDesc varchar_type;
    // {
    //     TTypeNode node;
    //     node.__set_type(TTypeNodeType::SCALAR);
    //     TScalarType scalar_type;
    //     scalar_type.__set_type(TPrimitiveType::VARCHAR);
    //     scalar_type.__set_len(65535);
    //     node.__set_scalar_type(scalar_type);
    //     varchar_type.types.push_back(node);
    // }
    for (int i = 0; i < _tablet->num_columns(); i++) {
        auto col = _tablet->tablet_schema()->column(i);
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(getPrimitiveType(col.type()));
            TPrimitiveType::type col_type = getPrimitiveType(col.type());
            if (col_type == TPrimitiveType::VARCHAR || col_type == TPrimitiveType::HLL
                || col_type == TPrimitiveType::CHAR || col_type == TPrimitiveType::STRING) {
                scalar_type.__set_len(col.length());
            } else if (col_type == TPrimitiveType::DECIMALV2 || col_type == TPrimitiveType::DECIMAL32 ||
                       col_type == TPrimitiveType::DECIMAL64 || col_type == TPrimitiveType::DECIMAL128I) {
                scalar_type.__set_precision(col.precision());
                scalar_type.__set_scale(col.frac());
            }
            node.__set_scalar_type(scalar_type);
            type.types.emplace_back(node);
        }
        // expr_of_dest_slot
        {

            TExpr expr;
            TPrimitiveType::type col_ptype = getPrimitiveType(col.type());
            if (col_ptype == TPrimitiveType::OBJECT || col_ptype == TPrimitiveType::HLL) {
//                do not need cast because  type is same
//                TExprNode cast_expr;
//                cast_expr.type = type;
//                cast_expr.num_children = 1;
//                // cast function info
//                TFunction tfunction;
//                TFunctionName fun_name;
//                fun_name.db_name = ; // todo: what?
//                fun_name.function_name = "castTo" + to_upper(to_string(getPrimitiveType(col.type()))); // todo: can we use to_string for enum PrimitiveType
//                tfunction.name = fun_name;
//                tfunction.arg_types
//                //TODO: core info about
//                cast_expr.vararg_start_idx = 0;
//                cast_expr.fn = tfunction;
//                // end for cast function info
//                cast_expr.node_type = TExprNodeType::CAST_EXPR;
//                cast_expr.opcode = TExprOpcode::CAST;
//                // TODO:cast_expr.output_column = ?;
//                expr.nodes.push_back(std::move(cast_expr));
                {
                    TExprNode transfer_expr;
                    transfer_expr.__set_type(type);
                    transfer_expr.__set_num_children(1);

                    TFunction transfer_func;
                    TScalarFunction trans_scalar_func;
                    TFunctionName fn_name;
                    if (col_ptype == TPrimitiveType::OBJECT) {
                        fn_name.__set_function_name("to_bitmap");
                        transfer_func.__set_signature("to_bitmap(TEXT)"); // TODO get the sig string
                        trans_scalar_func.__set_symbol("_ZN5doris15BitmapFunctions9to_bitmapEPN9doris_udf15FunctionContextERKNS1_9StringValE");

                    } else if (col_ptype == TPrimitiveType::HLL) {
                        fn_name.__set_function_name("hll_hash");
                        transfer_func.__set_signature("hll_hash(TEXT)"); // TODO get the sig string
                        trans_scalar_func.__set_symbol("_ZN5doris12HllFunctions8hll_hashEPN9doris_udf15FunctionContextERKNS1_9StringValE");
                    }

                    transfer_func.__set_name(fn_name);
                    transfer_func.__set_scalar_fn(trans_scalar_func);
                    transfer_func.__set_binary_type(TFunctionBinaryType::type::BUILTIN);
                    // args: list<string type>
                    {
                        std::vector<TTypeDesc> argTypes;
                        TTypeDesc argType;
                        TTypeNode node;
                        node.__set_type(TTypeNodeType::SCALAR);
                        TScalarType scalar_type;
                        scalar_type.__set_type(TPrimitiveType::STRING);
                        scalar_type.__set_len(0x7fffffff - 4);
                        node.__set_scalar_type(scalar_type);
                        argType.types.emplace_back(node);
                        argTypes.emplace_back(argType);
                        transfer_func.__set_arg_types(argTypes);
                    }
                    transfer_func.__set_ret_type(type); // todo check here
                    transfer_func.__set_has_var_args(false);
                    transfer_func.__set_id(0);
                    // built-in-function do not have checksum
                    transfer_func.__set_vectorized(true);
                    transfer_expr.__set_fn(transfer_func);
                    transfer_expr.__set_is_nullable(false);
                    transfer_expr.__set_output_scale(-1);
                    transfer_expr.__set_node_type(TExprNodeType::FUNCTION_CALL);
                    expr.nodes.push_back(transfer_expr);
                }

                {
                    TExprNode input_slot_ref;
                    input_slot_ref.__set_node_type(TExprNodeType::SLOT_REF);
                    input_slot_ref.__set_num_children(0);
                    {
                        TTypeDesc input_col_type;
                        TTypeNode node;
                        node.__set_type(TTypeNodeType::SCALAR);
                        TScalarType scalar_type;
                        scalar_type.__set_type(TPrimitiveType::STRING);
                        scalar_type.__set_len(0x7fffffff - 4);
                        node.__set_scalar_type(scalar_type);
                        input_col_type.types.emplace_back(node);
                        input_slot_ref.__set_type(type);
                    }
                    input_slot_ref.slot_ref.slot_id = _tablet->num_columns() + i; //TODO:check here
                    input_slot_ref.slot_ref.tuple_id = 1; // TODO: check here
                    input_slot_ref.__isset.slot_ref = true;
                    input_slot_ref.__set_is_nullable(false);
                    expr.nodes.push_back(input_slot_ref);
                }



            } else {
                TExprNode slot_ref;
                slot_ref.node_type = TExprNodeType::SLOT_REF;
                slot_ref.type = type;
                slot_ref.num_children = 0;
                slot_ref.slot_ref.slot_id = _tablet->num_columns() + i;
                slot_ref.slot_ref.tuple_id = 1;
                slot_ref.__isset.slot_ref = true;
                expr.nodes.push_back(slot_ref);
            }
            _params.expr_of_dest_slot.emplace(i, expr);
        }

        // default_value_of_src_slot
        {
            TExpr expr;
            TPrimitiveType::type col_ptype = getPrimitiveType(col.type());
            if (col_ptype == TPrimitiveType::OBJECT || col_ptype == TPrimitiveType::HLL) {
                //                do not need cast because  type is same
                //                TExprNode cast_expr;
                //                cast_expr.type = type;
                //                cast_expr.num_children = 1;
                //                // cast function info
                //                TFunction tfunction;
                //                TFunctionName fun_name;
                //                fun_name.db_name = ; // todo: what?
                //                fun_name.function_name = "castTo" + to_upper(to_string(getPrimitiveType(col.type()))); // todo: can we use to_string for enum PrimitiveType
                //                tfunction.name = fun_name;
                //                tfunction.arg_types
                //                //TODO: core info about
                //                cast_expr.vararg_start_idx = 0;
                //                cast_expr.fn = tfunction;
                //                // end for cast function info
                //                cast_expr.node_type = TExprNodeType::CAST_EXPR;
                //                cast_expr.opcode = TExprOpcode::CAST;
                //                // TODO:cast_expr.output_column = ?;
                //                expr.nodes.push_back(std::move(cast_expr));
                {
                    TExprNode transfer_expr;
                    transfer_expr.__set_type(type);
                    transfer_expr.__set_num_children(1);

                    TFunction transfer_func;
                    TScalarFunction trans_scalar_func;
                    TFunctionName fn_name;
                    if (col_ptype == TPrimitiveType::OBJECT) {
                        fn_name.__set_function_name("to_bitmap");
                        transfer_func.__set_signature("to_bitmap(TEXT)"); // TODO get the sig string
                        trans_scalar_func.__set_symbol(
                                "_ZN5doris15BitmapFunctions9to_bitmapEPN9doris_udf15FunctionContextERKNS1_9StringValE");

                    } else if (col_ptype == TPrimitiveType::HLL) {
                        fn_name.__set_function_name("hll_hash");
                        transfer_func.__set_signature("hll_hash(TEXT)"); // TODO get the sig string
                        trans_scalar_func.__set_symbol(
                                "_ZN5doris12HllFunctions8hll_hashEPN9doris_udf15FunctionContextERKNS1_9StringValE");
                    }

                    transfer_func.__set_name(fn_name);
                    transfer_func.__set_scalar_fn(trans_scalar_func);
                    transfer_func.__set_binary_type(TFunctionBinaryType::type::BUILTIN);
                    // args: list<string type>
                    {
                        std::vector<TTypeDesc> argTypes;
                        TTypeDesc argType;
                        TTypeNode node;
                        node.__set_type(TTypeNodeType::SCALAR);
                        TScalarType scalar_type;
                        scalar_type.__set_type(TPrimitiveType::STRING);
                        scalar_type.__set_len(0x7fffffff - 4);
                        node.__set_scalar_type(scalar_type);
                        argType.types.emplace_back(node);
                        argTypes.emplace_back(argType);
                        transfer_func.__set_arg_types(argTypes);
                    }
                    transfer_func.__set_ret_type(type); // todo check here
                    transfer_func.__set_has_var_args(false);
                    transfer_func.__set_id(0);
                    // built-in-function do not have checksum
                    transfer_func.__set_vectorized(true);
                    transfer_expr.__set_fn(transfer_func);
                    transfer_expr.__set_is_nullable(false);
                    transfer_expr.__set_output_scale(-1);
                    transfer_expr.__set_node_type(TExprNodeType::FUNCTION_CALL);
                    expr.nodes.push_back(transfer_expr);
                }

                {
                    TExprNode string_literal;
                    string_literal.__set_node_type(TExprNodeType::STRING_LITERAL);
                    string_literal.__set_num_children(0);
                    {
                        TTypeDesc string_literal_type;
                        TTypeNode node;
                        node.__set_type(TTypeNodeType::SCALAR);
                        TScalarType scalar_type;
                        scalar_type.__set_type(TPrimitiveType::STRING);
                        scalar_type.__set_len(0x7fffffff - 4);
                        node.__set_scalar_type(scalar_type);
                        string_literal_type.types.emplace_back(node);
                        string_literal.__set_type(string_literal_type);
                    }
                    string_literal.string_literal = TStringLiteral();
                    string_literal.string_literal.__set_value("");
                    string_literal.__isset.string_literal = true;
                    string_literal.__set_is_nullable(false); // fix function build check
                    expr.nodes.push_back(string_literal);
                }
            } else {
                TExprNode node;
                if (col.has_default_value()) {
                    node.node_type = TExprNodeType::STRING_LITERAL;
                    node.string_literal = TStringLiteral();
                    node.string_literal.__set_value(col.default_value());
                    node.__isset.string_literal = true;
                } else {
                    if (col.is_nullable()) {
                        node.node_type = TExprNodeType::NULL_LITERAL;
                    } else {
                        continue;
                    }
                }
                node.type = type;
                node.num_children = 0;
                expr.nodes.push_back(node);
            }

            _params.default_value_of_src_slot.emplace(i + _tablet->num_columns(), expr);
        }

        _params.dest_sid_to_src_sid_without_trans.emplace(i, _tablet->num_columns() + i);

        TFileScanSlotInfo slotInfo;
        slotInfo.slot_id = _tablet->num_columns() + i;
        slotInfo.is_file_slot = true;
        slotInfo.__isset.slot_id = true;
        slotInfo.__isset.is_file_slot = true;
        _params.required_slots.push_back(slotInfo);
    }

    _params.__isset.expr_of_dest_slot = true;
    _params.__isset.default_value_of_src_slot = true;
    _params.__isset.dest_sid_to_src_sid_without_trans = true;

    _params.__set_dest_tuple_id(TUPLE_ID_DST);
    _params.__set_src_tuple_id(TUPLE_ID_SRC);
    _params.format_type = TFileFormatType::FORMAT_PARQUET;
    _params.file_type = TFileType::FILE_LOCAL;
    _params.compress_type = TFileCompressType::PLAIN;
    _params.strict_mode = false;
    _params.num_of_columns_from_file = _tablet->num_columns();
}

void BuilderScannerMemtable::build_scan_ranges(
        std::vector<TFileRangeDesc>& ranges,
        const std::vector<std::filesystem::directory_entry>& files) {
    LOG(INFO) << "build scan ranges for files size:" << files.size() << " file_type:" << _file_type;
    for (const auto& file : files) {
        TFileRangeDesc range;
        range.path = file.path();
        range.start_offset = 0;
        range.size = file.file_size();
        range.file_size = range.size;
        ranges.push_back(range);
    }

    if (!ranges.size()) {
        LOG(FATAL) << "cannot get valid scan file!";
    }
}

void BuilderScannerMemtable::doSegmentBuild(
        const std::vector<std::filesystem::directory_entry>& files) {
    vectorized::NewFileScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    scan_node.init(_tnode, &_runtime_state);
    auto status = scan_node.prepare(&_runtime_state);
    if (!status.ok()) {
        LOG(FATAL) << "prepare scan node fail:" << status.to_string();
    }

    // set scan range
    std::vector<TScanRangeParams> scan_ranges;
    {
        TScanRangeParams scan_range_params;

        // TBrokerScanRange broker_scan_range;
        TFileScanRange file_scan_range;
        file_scan_range.__set_params(_params);
        // build_scan_ranges(broker_scan_range.ranges, files);
        // scan_range_params.scan_range.__set_broker_scan_range(broker_scan_range);
        build_scan_ranges(file_scan_range.ranges, files);
        TExternalScanRange ext_scan_range;
        ext_scan_range.file_scan_range = file_scan_range;
        scan_range_params.scan_range.__set_ext_scan_range(ext_scan_range);
        scan_ranges.push_back(scan_range_params);
    }

    scan_node.set_scan_ranges(scan_ranges);
    status = scan_node.open(&_runtime_state);
    if (!status.ok()) {
        LOG(FATAL) << "open scan node fail:" << status.to_string();
    }

    // std::unique_ptr<RowsetWriter> rowset_writer;
    PUniqueId load_id;
    load_id.set_hi(1);
    load_id.set_lo(1);
    int64_t transaction_id = 1;

    // delta writer
    TupleDescriptor* tuple_desc = _desc_tbl->get_tuple_descriptor(TUPLE_ID_DST);
    OlapTableSchemaParam param;
    WriteRequest write_req = {_tablet->tablet_meta()->tablet_id(),
                              _tablet->schema_hash(),
                              WriteType::LOAD,
                              transaction_id,
                              _tablet->partition_id(),
                              load_id,
                              tuple_desc,
                              &(tuple_desc->slots()),
                              false,
                              &param};

    DeltaWriter* delta_writer = nullptr;
    DeltaWriter::open(&write_req, &delta_writer, _runtime_state.runtime_profile(), load_id);
    status = delta_writer->init();
    if (!status.ok()) {
        LOG(FATAL) << "delta_writer init fail:" << status.to_string();
    }

    std::filesystem::path segment_path(std::filesystem::path(_build_dir + "/segment"));
    std::filesystem::remove_all(segment_path);
    if (!std::filesystem::create_directory(segment_path)) {
        LOG(FATAL) << "create segment path fail.";
    }

    delta_writer->set_writer_path(segment_path.string());
    // Get block
    vectorized::Block block;
    bool eof = false;

    std::vector<int> rowidx;
    for (size_t i = 0; i < BATCH_SIZE; ++i) {
        rowidx.push_back(i);
    }

    while (!eof) {
        status = scan_node.get_next(&_runtime_state, &block, &eof);
        if (!status.ok()) {
            LOG(FATAL) << "scan error: " << status.to_string();
            break;
        }

        if (block.rows() != BATCH_SIZE) {
            std::vector<int> index;
            for (size_t i = 0; i < block.rows(); ++i) {
                index.push_back(i);
            }
            status = delta_writer->write(&block, index);
        } else {
            status = delta_writer->write(&block, rowidx);
        }

        if (!status.ok()) {
            LOG(FATAL) << "add block error: " << status.to_string();
            break;
        }

        block.clear();
    }
    status = delta_writer->close();
    if (!status.ok()) {
        LOG(FATAL) << "delta_writer close error: " << status.to_string();
    }
    PSlaveTabletNodes slave_tablet_nodes;
    status = delta_writer->close_wait(slave_tablet_nodes, false);
    if (!status.ok()) {
        LOG(FATAL) << "delta_writer close_wait error: " << status.to_string();
    }

    RowsetMetaSharedPtr rowset_meta = delta_writer->get_cur_rowset()->rowset_meta();
    std::vector<RowsetMetaSharedPtr> metas {rowset_meta};

    _tablet->tablet_meta()->revise_rs_metas(std::move(metas));
    if (!status.ok()) {
        LOG(FATAL) << "cannot add new rowset: " << status.to_string();
    }

    scan_node.close(&_runtime_state);
    {
        std::stringstream ss;
        scan_node.runtime_profile()->pretty_print(&ss);
        LOG(INFO) << ss.str();
    }
    {
        std::stringstream ss;
        delta_writer->runtime_profile()->pretty_print(&ss);
        LOG(INFO) << ss.str();
    }
}

} // namespace doris
