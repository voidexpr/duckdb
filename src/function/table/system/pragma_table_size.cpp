#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/numeric_utils.hpp"

namespace duckdb {

struct PragmaTableSizeBindData : public TableFunctionData {
    explicit PragmaTableSizeBindData(TableCatalogEntry &table_p) : table(table_p) {
    }

    TableCatalogEntry &table;
};

struct PragmaTableSizeState : public GlobalTableFunctionState {
    PragmaTableSizeState() : returned(false) {
    }
    bool returned;
};

static unique_ptr<FunctionData> PragmaTableSizeBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
    names.emplace_back("table_name");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("table_size");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("block_size");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("total_blocks");
    return_types.emplace_back(LogicalType::BIGINT);

    auto qname = QualifiedName::Parse(input.inputs[0].GetValue<string>());
    Binder::BindSchemaOrCatalog(context, qname.catalog, qname.schema);
    auto &table = Catalog::GetEntry<TableCatalogEntry>(context, qname.catalog, qname.schema, qname.name);
    return make_uniq<PragmaTableSizeBindData>(table);
}

unique_ptr<GlobalTableFunctionState> PragmaTableSizeInit(ClientContext &context, TableFunctionInitInput &input) {
    return make_uniq<PragmaTableSizeState>();
}

void PragmaTableSizeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &bind_data = data_p.bind_data->Cast<PragmaTableSizeBindData>();
    auto &state = data_p.global_state->Cast<PragmaTableSizeState>();
    if (state.returned) {
        return;
    }

    unordered_set<block_id_t> blocks;
    auto segments = bind_data.table.GetColumnSegmentInfo();
    for (auto &info : segments) {
        if (!info.persistent) {
            continue;
        }
        blocks.insert(info.block_id);
        for (auto &bid : info.additional_blocks) {
            blocks.insert(bid);
        }
    }
    auto &block_manager = bind_data.table.GetStorage().GetTableIOManager().GetBlockManagerForRowData();
    idx_t block_size = block_manager.GetBlockSize();
    idx_t total_blocks = blocks.size();
    idx_t bytes = total_blocks * block_size;

    idx_t col = 0;
    output.data[col++].SetValue(0, Value(bind_data.table.name));
    output.data[col++].SetValue(0, Value(StringUtil::BytesToHumanReadableString(bytes)));
    output.data[col++].SetValue(0, Value::BIGINT(NumericCast<int64_t>(block_size)));
    output.data[col++].SetValue(0, Value::BIGINT(NumericCast<int64_t>(total_blocks)));
    output.SetCardinality(1);
    state.returned = true;
}

void PragmaTableSize::RegisterFunction(BuiltinFunctions &set) {
    set.AddFunction(TableFunction("pragma_table_size", {LogicalType::VARCHAR}, PragmaTableSizeFunction,
                                  PragmaTableSizeBind, PragmaTableSizeInit));
}

} // namespace duckdb
