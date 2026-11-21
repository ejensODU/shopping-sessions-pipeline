#pragma once

#include "aggregate.hpp"

#include <arrow/io/file.h>
#include <arrow/result.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/schema.h>

#include <filesystem>
#include <functional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace ss {
namespace output_detail {

using parquet::schema::NodePtr;
using parquet::schema::PrimitiveNode;

inline NodePtr text_field(const char* name, bool nullable = false) {
    return PrimitiveNode::Make(name, nullable ? parquet::Repetition::OPTIONAL :
        parquet::Repetition::REQUIRED, parquet::LogicalType::String(), parquet::Type::BYTE_ARRAY);
}
inline NodePtr day_field() {
    return PrimitiveNode::Make("day", parquet::Repetition::REQUIRED,
                               parquet::LogicalType::Date(), parquet::Type::INT32);
}
inline NodePtr count_field(const char* name) {
    return PrimitiveNode::Make(name, parquet::Repetition::REQUIRED, parquet::Type::INT64);
}

// The output path belongs to one worker. Write one column at a time so a large
// result never requires a second full-size copy in column-major order.
template<class Row, class Emit>
void write_file(const std::filesystem::path& path, const std::vector<Row>& rows,
                const parquet::schema::NodeVector& columns, Emit emit) {
    const auto schema = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, columns));
    const auto properties = parquet::WriterProperties::Builder()
        .disable_dictionary()->disable_statistics()->disable_write_page_index()
        ->compression(parquet::Compression::UNCOMPRESSED)->build();
    auto opened = arrow::io::FileOutputStream::Open(path.string());
    if (!opened.ok()) throw std::runtime_error(opened.status().ToString());
    auto sink = std::move(opened).ValueUnsafe();
    auto writer = parquet::ParquetFileWriter::Open(sink, schema, properties);
    constexpr size_t group_size = 64 * 1024;
    for (size_t i = 0; i < rows.size(); i += group_size) {
        auto* group = writer->AppendRowGroup();
        emit(*group, std::span<const Row>(rows).subspan(i, std::min(group_size, rows.size() - i)));
        group->Close();
    }
    writer->Close();
    const auto status = sink->Close();
    if (!status.ok()) throw std::runtime_error(status.ToString());
}

template<class DataType, class Row, class Extract>
void required(parquet::RowGroupWriter& group, std::span<const Row> rows, Extract extract) {
    std::vector<typename DataType::c_type> values;
    values.reserve(rows.size());
    for (const auto& row : rows) values.emplace_back(std::invoke(extract, row));
    auto* column = static_cast<parquet::TypedColumnWriter<DataType>*>(group.NextColumn());
    column->WriteBatch(rows.size(), nullptr, nullptr, values.data());
}

inline void categories_column(parquet::RowGroupWriter& group, std::span<const UserDay> rows,
                              const std::vector<std::string>& categories) {
    std::vector<int16_t> levels;
    std::vector<parquet::ByteArray> values;
    levels.reserve(rows.size());
    values.reserve(rows.size());
    for (const auto& row : rows) {
        const bool present = row.top >= 0;
        levels.push_back(present);
        if (present) values.emplace_back(std::string_view(categories[row.top]));
    }
    auto* column = static_cast<parquet::ByteArrayWriter*>(group.NextColumn());
    column->WriteBatch(rows.size(), levels.data(), nullptr, values.data());
}

} // namespace output_detail

inline void write_users(const std::filesystem::path& file, const std::vector<UserDay>& rows,
                        const std::vector<std::string>& names,
                        const std::vector<std::string>& categories) {
    using namespace output_detail;
    write_file(file, rows, {text_field("user_id"), day_field(), count_field("sessions"),
                            count_field("events"), count_field("revenue_cents"), text_field("top_category", true)},
               [&](parquet::RowGroupWriter& group, std::span<const UserDay> batch) {
        required<parquet::ByteArrayType>(group, batch, [&](const auto& r) {return std::string_view(names[r.user]);});
        required<parquet::Int32Type>(group, batch, &UserDay::day);
        required<parquet::Int64Type>(group, batch, &UserDay::sessions);
        required<parquet::Int64Type>(group, batch, &UserDay::events);
        required<parquet::Int64Type>(group, batch, &UserDay::revenue);
        categories_column(group, batch, categories);
    });
}

inline void write_campaigns(const std::filesystem::path& file, const std::vector<CampaignDay>& rows,
                            const std::vector<std::string>& campaigns,
                            const std::vector<std::string>& channels) {
    using namespace output_detail;
    write_file(file, rows, {text_field("campaign_id"), text_field("channel"), day_field(),
                            count_field("sessions"), count_field("purchasing_sessions"),
                            count_field("revenue_cents"), count_field("assisted_sessions")},
               [&](parquet::RowGroupWriter& group, std::span<const CampaignDay> batch) {
        required<parquet::ByteArrayType>(group, batch, [&](const auto& r) {return std::string_view(campaigns[r.campaign]);});
        required<parquet::ByteArrayType>(group, batch, [&](const auto& r) {return std::string_view(channels[r.campaign]);});
        required<parquet::Int32Type>(group, batch, &CampaignDay::day);
        required<parquet::Int64Type>(group, batch, &CampaignDay::sessions);
        required<parquet::Int64Type>(group, batch, &CampaignDay::purchasing);
        required<parquet::Int64Type>(group, batch, &CampaignDay::revenue);
        required<parquet::Int64Type>(group, batch, &CampaignDay::assists);
    });
}

} // namespace ss
