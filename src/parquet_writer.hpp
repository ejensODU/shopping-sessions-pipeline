#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unistd.h>
#include <vector>

namespace blitz::pq {

// Minimal zero-dependency Apache Parquet v1 writer (Thrift Compact Protocol,
// UNCOMPRESSED, PLAIN + RLE_DICTIONARY). Produces standard files readable by
// PyArrow, Polars, DuckDB, and Spark.

struct ByteBuf {
    std::vector<uint8_t> data;

    void clear() { data.clear(); }
    void reserve(size_t n) { data.reserve(n); }
    size_t size() const { return data.size(); }

    inline void push_u8(uint8_t v) { data.push_back(v); }

    inline void append(const void* src, size_t n) {
        if (!n) return;
        size_t old = data.size();
        data.resize(old + n);
        std::memcpy(data.data() + old, src, n);
    }

    inline void push_u16_le(uint16_t v) {
        uint8_t b[2] = {uint8_t(v), uint8_t(v >> 8)};
        append(b, 2);
    }

    inline void push_u32_le(uint32_t v) {
        uint8_t b[4] = {uint8_t(v), uint8_t(v >> 8), uint8_t(v >> 16), uint8_t(v >> 24)};
        append(b, 4);
    }

    inline void push_uvarint(uint64_t v) {
        while (v >= 0x80) {
            data.push_back(uint8_t((v & 0x7F) | 0x80));
            v >>= 7;
        }
        data.push_back(uint8_t(v));
    }

    inline void push_zigzag32(int32_t v) {
        uint32_t zz = (uint32_t(v) << 1) ^ uint32_t(v >> 31);
        push_uvarint(zz);
    }

    inline void push_zigzag64(int64_t v) {
        uint64_t zz = (uint64_t(v) << 1) ^ uint64_t(v >> 63);
        push_uvarint(zz);
    }
};

// Thrift Compact Protocol helper
class ThriftWriter {
    ByteBuf& buf_;
    int16_t last_field_ = 0;

    void field_header(int16_t id, uint8_t ctype) {
        int16_t delta = id - last_field_;
        if (delta > 0 && delta <= 15) {
            buf_.push_u8(uint8_t((delta << 4) | ctype));
        } else {
            buf_.push_u8(ctype);
            buf_.push_zigzag32(id);
        }
        last_field_ = id;
    }

public:
    static constexpr uint8_t CT_I32 = 5;
    static constexpr uint8_t CT_I64 = 6;
    static constexpr uint8_t CT_BINARY = 8;
    static constexpr uint8_t CT_LIST = 9;
    static constexpr uint8_t CT_STRUCT = 12;

    explicit ThriftWriter(ByteBuf& buf) : buf_(buf) {}

    void write_i32(int16_t id, int32_t v) {
        field_header(id, CT_I32);
        buf_.push_zigzag32(v);
    }

    void write_i64(int16_t id, int64_t v) {
        field_header(id, CT_I64);
        buf_.push_zigzag64(v);
    }

    void write_string(int16_t id, std::string_view s) {
        field_header(id, CT_BINARY);
        buf_.push_uvarint(s.size());
        buf_.append(s.data(), s.size());
    }

    void begin_struct(int16_t id) { field_header(id, CT_STRUCT); }

    void begin_list(int16_t id, uint8_t elem_type, size_t count) {
        field_header(id, CT_LIST);
        if (count < 15) {
            buf_.push_u8(uint8_t((count << 4) | elem_type));
        } else {
            buf_.push_u8(uint8_t(0xF0 | elem_type));
            buf_.push_uvarint(count);
        }
    }

    void end_struct() { buf_.push_u8(0); }

    ByteBuf& buf() { return buf_; }
};

// Parquet Thrift enums
constexpr int32_t PQ_INT32 = 1;
constexpr int32_t PQ_INT64 = 2;
constexpr int32_t PQ_BYTE_ARRAY = 6;

constexpr int32_t CONV_UTF8 = 0;
constexpr int32_t CONV_DATE = 6;

constexpr int32_t REP_REQUIRED = 0;
constexpr int32_t REP_OPTIONAL = 1;

constexpr int32_t ENC_PLAIN = 0;
constexpr int32_t ENC_RLE = 3;
constexpr int32_t ENC_RLE_DICTIONARY = 8;

constexpr int32_t CODEC_UNCOMPRESSED = 0;

constexpr int32_t PAGE_DATA = 0;
constexpr int32_t PAGE_DICTIONARY = 2;

struct ColumnSpec {
    std::string name;
    int32_t physical_type;
    int32_t repetition;
    int32_t converted_type; // -1 if none
    int16_t logical_field;  // 1=STRING, 6=DATE, 0=none
};

struct ColumnChunkMeta {
    int32_t type;
    std::vector<int32_t> encodings;
    std::string name;
    int64_t num_values;
    int64_t total_size;
    int64_t data_page_offset;
    int64_t dict_page_offset; // -1 if none
};

struct RowGroupMeta {
    std::vector<ColumnChunkMeta> columns;
    int64_t total_byte_size = 0;
    int64_t num_rows = 0;
};

inline void write_page_header_data(ByteBuf& out, int32_t payload_size, int32_t num_values, int32_t encoding) {
    ThriftWriter w(out);
    w.write_i32(1, PAGE_DATA);
    w.write_i32(2, payload_size);
    w.write_i32(3, payload_size);
    w.begin_struct(5);
    {
        ThriftWriter dp(out);
        dp.write_i32(1, num_values);
        dp.write_i32(2, encoding);
        dp.write_i32(3, ENC_RLE);
        dp.write_i32(4, ENC_RLE);
        dp.end_struct();
    }
    w.end_struct();
}

inline void write_page_header_dict(ByteBuf& out, int32_t payload_size, int32_t num_values) {
    ThriftWriter w(out);
    w.write_i32(1, PAGE_DICTIONARY);
    w.write_i32(2, payload_size);
    w.write_i32(3, payload_size);
    w.begin_struct(7);
    {
        ThriftWriter dp(out);
        dp.write_i32(1, num_values);
        dp.write_i32(2, ENC_PLAIN);
        dp.end_struct();
    }
    w.end_struct();
}

// Bit-pack `count` values of `bit_width` bits (1..16) in Parquet RLE-hybrid format
// (chunked into <= 63 groups of 8 values = 504 values per header for universal compatibility).
inline void encode_bitpacked_u32(ByteBuf& out, const uint32_t* vals, size_t count, int bit_width) {
    if (count == 0) return;
    size_t i = 0;
    while (i < count) {
        size_t rem = count - i;
        size_t groups = std::min<size_t>((rem + 7) / 8, 63);
        out.push_uvarint((groups << 1) | 1);
        size_t nvals = groups * 8;
        uint64_t acc = 0;
        int bits = 0;
        size_t out_bytes = groups * bit_width;
        size_t old = out.data.size();
        out.data.resize(old + out_bytes);
        uint8_t* dst = out.data.data() + old;
        for (size_t k = 0; k < nvals; ++k) {
            uint32_t v = (i + k < count) ? vals[i + k] : 0;
            acc |= uint64_t(v) << bits;
            bits += bit_width;
            while (bits >= 8) {
                *dst++ = uint8_t(acc);
                acc >>= 8;
                bits -= 8;
            }
        }
        i += std::min(rem, nvals);
    }
}

// Bit-pack 1-bit definition levels from packed byte bitmask (bit j of mask[j>>3] is level j).
inline void encode_def_levels_bitpacked(ByteBuf& out, const uint8_t* bitmask, size_t count) {
    // Write 4-byte placeholder for length
    size_t len_pos = out.size();
    out.push_u32_le(0);
    size_t start = out.size();
    size_t total_bytes = (count + 7) / 8;
    size_t pos = 0;
    while (pos < total_bytes) {
        size_t groups = std::min<size_t>(total_bytes - pos, 63);
        out.push_uvarint((groups << 1) | 1);
        out.append(bitmask + pos, groups);
        pos += groups;
    }
    uint32_t encoded_len = uint32_t(out.size() - start);
    std::memcpy(out.data.data() + len_pos, &encoded_len, 4);
}

inline int min_bit_width(uint32_t count) {
    if (count <= 2) return 1;
    return 32 - __builtin_clz(count - 1);
}

class ParquetFileWriter {
    int fd_ = -1;
    int64_t offset_ = 0;
    std::vector<ColumnSpec> schema_;
    std::vector<RowGroupMeta> row_groups_;
    int64_t total_rows_ = 0;
    ByteBuf io_buf_;
    ByteBuf hdr_buf_;

    void flush_raw(const void* data, size_t n) {
        const uint8_t* p = static_cast<const uint8_t*>(data);
        while (n > 0) {
            ssize_t w = ::write(fd_, p, n);
            if (w <= 0) throw std::runtime_error("Parquet write failed");
            p += w;
            n -= size_t(w);
        }
    }

    void write_bytes(const void* data, size_t n) {
        if (io_buf_.size() + n > (1u << 20)) {
            if (!io_buf_.data.empty()) {
                flush_raw(io_buf_.data.data(), io_buf_.size());
                io_buf_.clear();
            }
            if (n >= (512u << 10)) {
                flush_raw(data, n);
                offset_ += int64_t(n);
                return;
            }
        }
        io_buf_.append(data, n);
        offset_ += int64_t(n);
    }

public:
    ParquetFileWriter(const std::string& path, std::vector<ColumnSpec> schema)
        : schema_(std::move(schema)) {
        fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (fd_ < 0) throw std::runtime_error("Could not open output " + path);
        io_buf_.reserve(1u << 20);
        write_bytes("PAR1", 4);
    }

    ~ParquetFileWriter() {
        if (fd_ >= 0) ::close(fd_);
    }

    ParquetFileWriter(const ParquetFileWriter&) = delete;
    ParquetFileWriter& operator=(const ParquetFileWriter&) = delete;

    RowGroupMeta& begin_row_group(int64_t num_rows) {
        row_groups_.emplace_back();
        auto& rg = row_groups_.back();
        rg.columns.reserve(schema_.size());
        rg.num_rows = num_rows;
        total_rows_ += num_rows;
        return rg;
    }

    void write_plain_column(RowGroupMeta& rg, size_t col_idx, const void* data, size_t byte_len, int64_t num_values) {
        const auto& spec = schema_[col_idx];
        int64_t start_off = offset_;
        hdr_buf_.clear();
        write_page_header_data(hdr_buf_, int32_t(byte_len), int32_t(num_values), ENC_PLAIN);
        write_bytes(hdr_buf_.data.data(), hdr_buf_.size());
        write_bytes(data, byte_len);
        int64_t col_size = offset_ - start_off;
        rg.total_byte_size += col_size;
        rg.columns.push_back({spec.physical_type, {ENC_PLAIN}, spec.name, num_values, col_size, start_off, -1});
    }

    void write_dict_column(RowGroupMeta& rg, size_t col_idx,
                           const void* dict_data, size_t dict_bytes, int32_t dict_count,
                           const void* page_data, size_t page_bytes, int64_t num_values) {
        const auto& spec = schema_[col_idx];
        int64_t dict_off = offset_;
        hdr_buf_.clear();
        write_page_header_dict(hdr_buf_, int32_t(dict_bytes), dict_count);
        write_bytes(hdr_buf_.data.data(), hdr_buf_.size());
        write_bytes(dict_data, dict_bytes);

        int64_t data_off = offset_;
        hdr_buf_.clear();
        write_page_header_data(hdr_buf_, int32_t(page_bytes), int32_t(num_values), ENC_RLE_DICTIONARY);
        write_bytes(hdr_buf_.data.data(), hdr_buf_.size());
        write_bytes(page_data, page_bytes);

        int64_t col_size = offset_ - dict_off;
        rg.total_byte_size += col_size;
        rg.columns.push_back({spec.physical_type, {ENC_PLAIN, ENC_RLE_DICTIONARY}, spec.name,
                              num_values, col_size, data_off, dict_off});
    }

    void close() {
        if (fd_ < 0) return;
        ByteBuf meta;
        meta.reserve(4096);
        {
            ThriftWriter fm(meta);
            fm.write_i32(1, 1); // version 1
            // schema: root + columns
            fm.begin_list(2, ThriftWriter::CT_STRUCT, 1 + schema_.size());
            {
                ThriftWriter root(meta);
                root.write_string(4, "schema");
                root.write_i32(5, int32_t(schema_.size()));
                root.end_struct();
            }
            for (const auto& col : schema_) {
                ThriftWriter se(meta);
                se.write_i32(1, col.physical_type);
                se.write_i32(3, col.repetition);
                se.write_string(4, col.name);
                if (col.converted_type >= 0) se.write_i32(6, col.converted_type);
                if (col.logical_field > 0) {
                    se.begin_struct(10);
                    {
                        ThriftWriter lt(meta);
                        lt.begin_struct(col.logical_field);
                        meta.push_u8(0); // empty logical type struct
                        lt.end_struct();
                    }
                }
                se.end_struct();
            }
            fm.write_i64(3, total_rows_);
            fm.begin_list(4, ThriftWriter::CT_STRUCT, row_groups_.size());
            for (const auto& rg : row_groups_) {
                ThriftWriter rgw(meta);
                rgw.begin_list(1, ThriftWriter::CT_STRUCT, rg.columns.size());
                for (const auto& cc : rg.columns) {
                    ThriftWriter ccw(meta);
                    ccw.write_i64(2, cc.dict_page_offset >= 0 ? cc.dict_page_offset : cc.data_page_offset);
                    ccw.begin_struct(3);
                    {
                        ThriftWriter cm(meta);
                        cm.write_i32(1, cc.type);
                        cm.begin_list(2, ThriftWriter::CT_I32, cc.encodings.size());
                        for (int32_t e : cc.encodings) meta.push_zigzag32(e);
                        cm.begin_list(3, ThriftWriter::CT_BINARY, 1);
                        meta.push_uvarint(cc.name.size());
                        meta.append(cc.name.data(), cc.name.size());
                        cm.write_i32(4, CODEC_UNCOMPRESSED);
                        cm.write_i64(5, cc.num_values);
                        cm.write_i64(6, cc.total_size);
                        cm.write_i64(7, cc.total_size);
                        cm.write_i64(9, cc.data_page_offset);
                        if (cc.dict_page_offset >= 0) cm.write_i64(11, cc.dict_page_offset);
                        cm.end_struct();
                    }
                    ccw.end_struct();
                }
                rgw.write_i64(2, rg.total_byte_size);
                rgw.write_i64(3, rg.num_rows);
                rgw.end_struct();
            }
            fm.end_struct();
        }
        uint32_t meta_len = uint32_t(meta.size());
        write_bytes(meta.data.data(), meta.size());
        uint8_t tail[8];
        std::memcpy(tail, &meta_len, 4);
        std::memcpy(tail + 4, "PAR1", 4);
        write_bytes(tail, 8);
        if (!io_buf_.data.empty()) {
            flush_raw(io_buf_.data.data(), io_buf_.size());
            io_buf_.clear();
        }
        if (::close(fd_) != 0) throw std::runtime_error("Parquet close failed");
        fd_ = -1;
    }
};

} // namespace blitz::pq
