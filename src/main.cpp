#include "arena.hpp"
#include "catalog_people.hpp"
#include "io.hpp"
#include "keys.hpp"
#include "parquet_writer.hpp"
#include "parse.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/resource.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace blitz {

namespace fs = std::filesystem;

constexpr size_t SHARD_BITS = 7;
constexpr size_t SHARDS = size_t{1} << SHARD_BITS; // 128

constexpr int64_t GAP_MS = 1'800'000;
constexpr int64_t REACH_MS = 604'800'000;

constexpr uint32_t INVALID_LOC = 0xFFFFFFFFu;
constexpr uint32_t DEAD_TS_U32 = 0xFFFFFFFFu;
constexpr int64_t DEAD_TS_I64 = std::numeric_limits<int64_t>::min();

struct Window {
    int64_t start = 0;
    int64_t end = 0;

    explicit Window(const fs::path& dir) {
        std::ifstream f(dir / "manifest.json");
        if (!f) throw std::runtime_error("Cannot open manifest.json");
        std::string json{std::istreambuf_iterator<char>(f), {}};
        auto extract = [&](const char* key) -> int64_t {
            std::string pat = std::string("\"") + key + "\"";
            size_t p = json.find(pat);
            if (p == std::string::npos) throw std::runtime_error(std::string("Missing ") + key);
            p = json.find(':', p + pat.size());
            if (p == std::string::npos) throw std::runtime_error("Malformed manifest.json");
            size_t q1 = json.find('"', p + 1);
            size_t q2 = (q1 == std::string::npos) ? std::string::npos : json.find('"', q1 + 1);
            if (q1 == std::string::npos || q2 == std::string::npos)
                throw std::runtime_error("Malformed manifest value");
            SV val(json.data() + q1 + 1, q2 - q1 - 1);
            int64_t t = time_detail::parse_iso_general(val);
            if (t == INVALID_TS) t = time_detail::parse_f3_general(val);
            if (t == INVALID_TS) throw std::runtime_error("Invalid manifest timestamp");
            return t;
        };
        start = extract("window_start");
        end = extract("window_end");
        if (end <= start) throw std::runtime_error("Empty window");
    }
};

// ---------------- Compact Mode Types (16B event, 16B aligned dedup, 24B click) ----------------
struct CompactEv {
    uint64_t person;    // exact 64-bit RawKeys<'u'>
    uint32_t ts_off;    // ms since win.start; DEAD_TS_U32 if deduplicated loser
    uint32_t price_cat; // (category << 24) | price
};
static_assert(sizeof(CompactEv) == 16);

struct CompactDedup {
    uint64_t ev_key;     // exact 64-bit RawKeys<'e'> (ALWAYS != 0)
    uint32_t ingest_off; // seconds since ingest_epoch_sec (full 32 bits = 136 years!)
    uint32_t loc;        // (worker << 28) | (local_slab_id << 10) | idx
};
static_assert(sizeof(CompactDedup) == 16);

struct CompactClick {
    uint64_t person;   // exact 64-bit RawKeys<'u'>
    uint64_t click_id; // exact 64-bit ClickKeys (1-bswap bytewise order!)
    uint32_t ts_off;   // ms since (win.start - 7*DAY_MS)
    uint32_t campaign;
};
static_assert(sizeof(CompactClick) == 24);

struct PackedClick {
    uint64_t click_id;
    uint32_t ts_off;
    uint32_t campaign;
};
static_assert(sizeof(PackedClick) == 16);

// ---------------- Wide Mode Types ----------------
struct WideEv {
    uint64_t person;
    int64_t ts;
    uint32_t price;
    uint32_t category;
};
static_assert(sizeof(WideEv) == 24);

struct WideDedup {
    uint64_t ev_key;
    int64_t ingest;
    uint32_t loc;
    uint32_t pad;
};
static_assert(sizeof(WideDedup) == 24);

struct WideClick {
    uint64_t person;
    uint64_t click_id;
    int64_t ts;
    uint32_t campaign;
    uint32_t pad;
};
static_assert(sizeof(WideClick) == 32);

// ---------------- Transposed SoA Campaign Totals ([camp][day]) ----------------
// `credit_dense[camp * days + d]` (16B): organic + whale campaigns live in ~1.3 KiB of L1d!
// `assists_dense[camp * days + d]` (4B): all 14 days of a campaign fit in ONE 64B cache line!
struct CreditAcc {
    uint64_t revenue = 0;
    uint32_t sessions = 0;
    uint32_t purchasing = 0;
};
static_assert(sizeof(CreditAcc) == 16);

struct SparseCampAcc {
    uint64_t revenue = 0;
    uint32_t sessions = 0;
    uint32_t purchasing = 0;
    uint32_t assists = 0;
};

struct CampaignLayout {
    int32_t first_day = 0;
    uint32_t days = 0;
    uint32_t campaigns = 0;
    bool dense = false;
};

class WorkerCampaigns {
    CampaignLayout layout_;
    std::vector<CreditAcc> credit_dense_;
    std::vector<uint32_t> assists_dense_;
    std::unordered_map<uint64_t, SparseCampAcc> sparse_;

public:
    explicit WorkerCampaigns(CampaignLayout layout) : layout_(layout) {
        if (layout.dense) {
            const size_t n = size_t(layout.campaigns) * layout.days;
            credit_dense_.resize(n);
            assists_dense_.resize(n, 0);
        }
    }

    inline void add_credit(uint32_t camp, int32_t day, int64_t rev) {
        if (__builtin_expect(layout_.dense, 1)) {
            auto& acc = credit_dense_[size_t(camp) * layout_.days + size_t(day - layout_.first_day)];
            ++acc.sessions;
            acc.purchasing += (rev > 0);
            acc.revenue += uint64_t(rev);
            return;
        }
        uint64_t k = (uint64_t(uint32_t(day)) << 32) | camp;
        auto& acc = sparse_[k];
        ++acc.sessions;
        acc.purchasing += (rev > 0);
        acc.revenue += uint64_t(rev);
    }

    inline void add_organic(int32_t day, int64_t rev) {
        if (__builtin_expect(layout_.dense, 1)) {
            auto& acc = credit_dense_[size_t(day - layout_.first_day)]; // camp == 0!
            ++acc.sessions;
            acc.purchasing += (rev > 0);
            acc.revenue += uint64_t(rev);
            return;
        }
        add_credit(0u, day, rev);
    }

    inline void add_assists(uint32_t camp, int32_t day, uint32_t count) {
        if (__builtin_expect(layout_.dense, 1)) {
            assists_dense_[size_t(camp) * layout_.days + size_t(day - layout_.first_day)] += count;
            return;
        }
        uint64_t k = (uint64_t(uint32_t(day)) << 32) | camp;
        sparse_[k].assists += count;
    }

    inline bool is_dense() const { return layout_.dense; }
    inline CreditAcc* credit_row(uint32_t camp) {
        return credit_dense_.data() + size_t(camp) * layout_.days;
    }
    inline uint32_t* assists_row(uint32_t camp) {
        return assists_dense_.data() + size_t(camp) * layout_.days;
    }

    const std::vector<CreditAcc>& credit_dense() const { return credit_dense_; }
    const std::vector<uint32_t>& assists_dense() const { return assists_dense_; }
    const std::unordered_map<uint64_t, SparseCampAcc>& sparse() const { return sparse_; }
};

// ---------------- User Daily Columnar Parquet Buffer (Zero-Arrow) ----------------
inline std::vector<pq::ColumnSpec> user_daily_schema() {
    using namespace pq;
    return {
        {"user_id", PQ_BYTE_ARRAY, REP_REQUIRED, CONV_UTF8, 1},
        {"day", PQ_INT32, REP_REQUIRED, CONV_DATE, 6},
        {"sessions", PQ_INT32, REP_REQUIRED, -1, 0},
        {"events", PQ_INT32, REP_REQUIRED, -1, 0},
        {"revenue_cents", PQ_INT64, REP_REQUIRED, -1, 0},
        {"top_category", PQ_BYTE_ARRAY, REP_OPTIONAL, CONV_UTF8, 1},
    };
}

inline std::vector<pq::ColumnSpec> campaign_daily_schema() {
    using namespace pq;
    return {
        {"campaign_id", PQ_BYTE_ARRAY, REP_REQUIRED, CONV_UTF8, 1},
        {"channel", PQ_BYTE_ARRAY, REP_REQUIRED, CONV_UTF8, 1},
        {"day", PQ_INT32, REP_REQUIRED, CONV_DATE, 6},
        {"sessions", PQ_INT64, REP_REQUIRED, -1, 0},
        {"purchasing_sessions", PQ_INT64, REP_REQUIRED, -1, 0},
        {"revenue_cents", PQ_INT64, REP_REQUIRED, -1, 0},
        {"assisted_sessions", PQ_INT64, REP_REQUIRED, -1, 0},
    };
}

class UserParquetStream {
    static constexpr size_t MAX_RG_ROWS = 131072;
    static constexpr uint32_t MAX_RG_USERS = 65000;

    std::string path_;
    std::unique_ptr<pq::ParquetFileWriter> writer_;
    const pq::ByteBuf& cat_dict_;
    int32_t cat_count_;
    int cat_bit_width_;

    bool use_day_dict_ = false;
    int32_t first_day_ = 0;
    uint32_t num_days_ = 0;
    int day_bit_width_ = 4;
    std::vector<int32_t> day_dict_vals_;

    pq::ByteBuf udict_;
    pq::ByteBuf upage_;
    uint32_t rg_users_ = 0;
    size_t rg_rows_ = 0;

    std::vector<int32_t> days_;
    std::vector<uint32_t> day_idx_;
    std::vector<int32_t> sessions_;
    std::vector<int32_t> events_;
    std::vector<int64_t> revenue_;
    std::vector<uint8_t> top_def_mask_;
    std::vector<uint32_t> top_vals_;
    pq::ByteBuf tmp_page_;

    void reset_rg() {
        udict_.clear();
        upage_.clear();
        upage_.push_u8(16);
        rg_users_ = 0;
        rg_rows_ = 0;
        top_vals_.clear();
    }

public:
    bool wrote_any = false;

    UserParquetStream(std::string path, const pq::ByteBuf& cat_dict, int32_t cat_count,
                      int32_t first_day, uint32_t num_days)
        : path_(std::move(path)),
          cat_dict_(cat_dict),
          cat_count_(cat_count),
          cat_bit_width_(pq::min_bit_width(uint32_t(cat_count))),
          use_day_dict_(num_days > 0 && num_days <= 32),
          first_day_(first_day),
          num_days_(num_days),
          day_bit_width_(pq::min_bit_width(std::max(1u, num_days))) {
        if (use_day_dict_) {
            day_dict_vals_.resize(num_days);
            for (uint32_t i = 0; i < num_days; ++i) day_dict_vals_[i] = first_day + int32_t(i);
            day_idx_.resize(MAX_RG_ROWS + 4096);
        } else {
            days_.resize(MAX_RG_ROWS + 4096);
        }
        sessions_.resize(MAX_RG_ROWS + 4096);
        events_.resize(MAX_RG_ROWS + 4096);
        revenue_.resize(MAX_RG_ROWS + 4096);
        top_def_mask_.resize((MAX_RG_ROWS + 4096) / 8 + 16, 0);
        udict_.reserve(1u << 20);
        upage_.reserve(256u << 10);
        top_vals_.reserve(32768);
        tmp_page_.reserve(128u << 10);
        reset_rg();
    }

    inline void begin_user(SV user_str, uint32_t day_count) {
        if (__builtin_expect(rg_rows_ + day_count > MAX_RG_ROWS || rg_users_ >= MAX_RG_USERS, 0)) {
            if (rg_rows_ > 0) flush_rg();
            if (__builtin_expect(day_count > sessions_.size(), 0)) {
                size_t cap = day_count + 1024;
                if (use_day_dict_) day_idx_.resize(cap); else days_.resize(cap);
                sessions_.resize(cap);
                events_.resize(cap);
                revenue_.resize(cap);
                top_def_mask_.resize(cap / 8 + 16, 0);
            }
        }
        uint32_t uid = rg_users_++;
        udict_.push_u32_le(uint32_t(user_str.size()));
        udict_.append(user_str.data(), user_str.size());
        upage_.push_uvarint(uint64_t(day_count) << 1);
        upage_.push_u16_le(uint16_t(uid));
    }

    inline void add_day(int32_t day, int32_t sess, int32_t evs, int64_t rev, int32_t top_cat) {
        const size_t r = rg_rows_++;
        if (use_day_dict_) day_idx_[r] = uint32_t(day - first_day_);
        else days_[r] = day;
        sessions_[r] = sess;
        events_[r] = evs;
        revenue_[r] = rev;
        if ((r & 7u) == 0) top_def_mask_[r >> 3] = 0;
        if (top_cat >= 0) {
            top_def_mask_[r >> 3] |= uint8_t(1u << (r & 7u));
            top_vals_.push_back(uint32_t(top_cat));
        }
    }

    void flush_rg() {
        if (rg_rows_ == 0) return;
        if (!writer_) {
            writer_ = std::make_unique<pq::ParquetFileWriter>(path_, user_daily_schema());
        }
        wrote_any = true;
        auto& rg = writer_->begin_row_group(int64_t(rg_rows_));
        writer_->write_dict_column(rg, 0, udict_.data.data(), udict_.size(), int32_t(rg_users_),
                                   upage_.data.data(), upage_.size(), int64_t(rg_rows_));
        if (use_day_dict_) {
            tmp_page_.clear();
            tmp_page_.push_u8(uint8_t(day_bit_width_));
            pq::encode_bitpacked_u32(tmp_page_, day_idx_.data(), rg_rows_, day_bit_width_);
            writer_->write_dict_column(rg, 1, day_dict_vals_.data(), day_dict_vals_.size() * sizeof(int32_t),
                                       int32_t(num_days_), tmp_page_.data.data(), tmp_page_.size(), int64_t(rg_rows_));
        } else {
            writer_->write_plain_column(rg, 1, days_.data(), rg_rows_ * sizeof(int32_t), int64_t(rg_rows_));
        }
        writer_->write_plain_column(rg, 2, sessions_.data(), rg_rows_ * sizeof(int32_t), int64_t(rg_rows_));
        writer_->write_plain_column(rg, 3, events_.data(), rg_rows_ * sizeof(int32_t), int64_t(rg_rows_));
        writer_->write_plain_column(rg, 4, revenue_.data(), rg_rows_ * sizeof(int64_t), int64_t(rg_rows_));

        tmp_page_.clear();
        pq::encode_def_levels_bitpacked(tmp_page_, top_def_mask_.data(), rg_rows_);
        if (top_vals_.empty()) {
            writer_->write_plain_column(rg, 5, tmp_page_.data.data(), tmp_page_.size(), int64_t(rg_rows_));
        } else {
            tmp_page_.push_u8(uint8_t(cat_bit_width_));
            pq::encode_bitpacked_u32(tmp_page_, top_vals_.data(), top_vals_.size(), cat_bit_width_);
            writer_->write_dict_column(rg, 5, cat_dict_.data.data(), cat_dict_.size(), cat_count_,
                                       tmp_page_.data.data(), tmp_page_.size(), int64_t(rg_rows_));
        }
        reset_rg();
    }

    void finish() {
        flush_rg();
        if (writer_) writer_->close();
    }
};

// ---------------- Fast Parsing Helpers ----------------
inline int parse_event_type(SV s) {
    const size_t n = s.size();
    if (n == 4) {
        uint32_t w;
        std::memcpy(&w, s.data(), 4);
        if (w == 0x77656976u || w == 0x74726163u) return 1;
        return 0;
    }
    if (n == 8) {
        uint64_t w;
        std::memcpy(&w, s.data(), 8);
        if (w == 0x6573616863727570ULL) return 2;
    }
    return 0;
}

inline bool skip_delim_comma(const char*& p, const char* s, size_t L, size_t q) {
    if (__builtin_expect((s[L] == (q ? '"' : ',')) & (!q | (s[L + 1] == ',')), 1)) {
        p = s + L + 1 + q;
        return true;
    }
    return false;
}

inline bool skip_delim_nl(const char*& p, const char* s, size_t L, size_t q) {
    if (__builtin_expect((s[L] == (q ? '"' : '\n')) & (!q | (s[L + 1] == '\n')), 1)) {
        p = s + L + 1 + q;
        return true;
    }
    return false;
}

// ---------------- Daily Category Aggregator ----------------
class DailyCats {
    std::vector<int64_t> sums_;
    std::vector<uint32_t> touched_;

public:
    explicit DailyCats(size_t ncats) : sums_(ncats, 0) { touched_.reserve(32); }

    inline void add(uint32_t cat, uint32_t price) {
        if (__builtin_expect(cat >= sums_.size(), 0)) sums_.resize(cat + 1, 0);
        if (sums_[cat] == 0) touched_.push_back(cat);
        sums_[cat] += price;
    }

    inline int32_t finish_day() {
        if (touched_.empty()) return -1;
        int32_t best = -1;
        int64_t best_val = 0;
        for (uint32_t c : touched_) {
            int64_t v = sums_[c];
            sums_[c] = 0;
            if (v > best_val || (v == best_val && c < uint32_t(best))) {
                best_val = v;
                best = int32_t(c);
            }
        }
        touched_.clear();
        return best;
    }
};

struct TempUserDay {
    int32_t day;
    int32_t sessions;
    int32_t events;
    int64_t revenue;
    int32_t top_cat;
};

struct SessionRec {
    int64_t start;
    int64_t revenue;
    int32_t day;
    uint32_t day_end;
};

struct CoverageRec {
    uint32_t gen;
    uint32_t end;
};

// ---------------- Ultra-Fast Branchless 8-Byte Packed Event Sort ----------------
// Branchless 2..8 sorting networks + branchless Lomuto partition (depth-limited O(n log n))
// for n < 512, and 8-bit/11-bit LSD radix sort on top 32 bits (ts_off) for n >= 512.
namespace sort_detail {

inline void cswap64(uint64_t& a, uint64_t& b) {
    uint64_t lo = a < b ? a : b;
    uint64_t hi = a < b ? b : a;
    a = lo;
    b = hi;
}

inline void net2(uint64_t* d) { cswap64(d[0], d[1]); }
inline void net3(uint64_t* d) { cswap64(d[0], d[1]); cswap64(d[1], d[2]); cswap64(d[0], d[1]); }
inline void net4(uint64_t* d) {
    cswap64(d[0], d[1]); cswap64(d[2], d[3]);
    cswap64(d[0], d[2]); cswap64(d[1], d[3]);
    cswap64(d[1], d[2]);
}
inline void net5(uint64_t* d) {
    cswap64(d[0], d[1]); cswap64(d[3], d[4]); cswap64(d[2], d[4]);
    cswap64(d[2], d[3]); cswap64(d[1], d[4]); cswap64(d[0], d[3]);
    cswap64(d[0], d[2]); cswap64(d[1], d[3]); cswap64(d[1], d[2]);
}
inline void net6(uint64_t* d) {
    cswap64(d[1], d[2]); cswap64(d[4], d[5]); cswap64(d[0], d[2]); cswap64(d[3], d[5]);
    cswap64(d[0], d[1]); cswap64(d[3], d[4]); cswap64(d[2], d[5]); cswap64(d[0], d[3]);
    cswap64(d[1], d[4]); cswap64(d[2], d[4]); cswap64(d[1], d[3]); cswap64(d[2], d[3]);
}
inline void net7(uint64_t* d) {
    cswap64(d[1], d[2]); cswap64(d[3], d[4]); cswap64(d[5], d[6]);
    cswap64(d[0], d[2]); cswap64(d[3], d[5]); cswap64(d[4], d[6]);
    cswap64(d[0], d[1]); cswap64(d[4], d[5]); cswap64(d[2], d[6]);
    cswap64(d[0], d[4]); cswap64(d[1], d[5]);
    cswap64(d[0], d[3]); cswap64(d[2], d[5]);
    cswap64(d[1], d[3]); cswap64(d[2], d[4]);
    cswap64(d[2], d[3]);
}
inline void net8(uint64_t* d) {
    cswap64(d[0], d[1]); cswap64(d[2], d[3]); cswap64(d[4], d[5]); cswap64(d[6], d[7]);
    cswap64(d[0], d[2]); cswap64(d[1], d[3]); cswap64(d[4], d[6]); cswap64(d[5], d[7]);
    cswap64(d[1], d[2]); cswap64(d[5], d[6]);
    cswap64(d[0], d[4]); cswap64(d[3], d[7]);
    cswap64(d[1], d[5]); cswap64(d[2], d[6]);
    cswap64(d[1], d[4]); cswap64(d[3], d[6]);
    cswap64(d[2], d[4]); cswap64(d[3], d[5]);
    cswap64(d[3], d[4]);
}

inline size_t branchless_partition64(uint64_t* d, size_t n) {
    const size_t mid = n >> 1;
    cswap64(d[0], d[mid]);
    cswap64(d[mid], d[n - 1]);
    cswap64(d[0], d[mid]);
    const uint64_t piv = d[mid];
    std::swap(d[mid], d[n - 1]);
    size_t i = 0;
    for (size_t j = 0; j < n - 1; ++j) {
        const uint64_t dj = d[j];
        const uint64_t di = d[i];
        const bool lt = dj < piv;
        d[j] = lt ? di : dj;
        d[i] = lt ? dj : di;
        i += lt;
    }
    std::swap(d[i], d[n - 1]);
    return i;
}

inline void qsort_net64(uint64_t* d, size_t n, int depth) {
    while (n > 8) {
        if (__builtin_expect(depth == 0, 0)) {
            std::sort(d, d + n);
            return;
        }
        --depth;
        size_t p = branchless_partition64(d, n);
        if (p < n - 1 - p) {
            qsort_net64(d, p, depth);
            d += p + 1;
            n -= p + 1;
        } else {
            qsort_net64(d + p + 1, n - 1 - p, depth);
            n = p;
        }
    }
    switch (n) {
        case 2: net2(d); break;
        case 3: net3(d); break;
        case 4: net4(d); break;
        case 5: net5(d); break;
        case 6: net6(d); break;
        case 7: net7(d); break;
        case 8: net8(d); break;
        default: break;
    }
}

inline void cswap_cl(PackedClick& a, PackedClick& b) {
    if (b.ts_off < a.ts_off) std::swap(a, b);
}

inline void sort_packed_clicks(PackedClick* d, size_t n) {
    if (n <= 1) return;
    if (n <= 4) {
        if (n == 2) { cswap_cl(d[0], d[1]); return; }
        if (n == 3) { cswap_cl(d[0], d[1]); cswap_cl(d[1], d[2]); cswap_cl(d[0], d[1]); return; }
        cswap_cl(d[0], d[1]); cswap_cl(d[2], d[3]);
        cswap_cl(d[0], d[2]); cswap_cl(d[1], d[3]);
        cswap_cl(d[1], d[2]);
        return;
    }
    if (n <= 24) {
        for (size_t i = 1; i < n; ++i) {
            PackedClick key = d[i];
            size_t j = i;
            while (j > 0 && d[j - 1].ts_off > key.ts_off) {
                d[j] = d[j - 1];
                --j;
            }
            d[j] = key;
        }
        return;
    }
    std::sort(d, d + n, [](const PackedClick& a, const PackedClick& b) {
        return a.ts_off < b.ts_off;
    });
}

} // namespace sort_detail

inline void sort_packed_events(uint64_t* data, size_t n, std::vector<uint64_t>& tmp) {
    if (n <= 1) return;
    if (n < 512) {
        sort_detail::qsort_net64(data, n, 18);
        return;
    }
    tmp.resize(n);
    if (n < 4096) {
        uint32_t cnt[256];
        uint64_t* src = data;
        uint64_t* dst = tmp.data();
        for (unsigned shift = 32; shift < 64; shift += 8) {
            uint32_t b0 = uint32_t((src[0] >> shift) & 255u);
            bool vary = false;
            std::memset(cnt, 0, sizeof(cnt));
            for (size_t i = 0; i < n; ++i) {
                uint32_t b = uint32_t((src[i] >> shift) & 255u);
                vary |= (b != b0);
                ++cnt[b];
            }
            if (!vary) continue;
            uint32_t sum = 0;
            for (uint32_t b = 0; b < 256; ++b) {
                uint32_t c = cnt[b];
                cnt[b] = sum;
                sum += c;
            }
            for (size_t i = 0; i < n; ++i) dst[cnt[(src[i] >> shift) & 255u]++] = src[i];
            std::swap(src, dst);
        }
        if (src != data) std::memcpy(data, src, n * sizeof(uint64_t));
        return;
    }
    uint32_t cnt[2048];
    auto pass = [&](uint64_t* src, uint64_t* dst, unsigned shift, uint32_t mask, uint32_t buckets) {
        std::memset(cnt, 0, buckets * sizeof(uint32_t));
        for (size_t i = 0; i < n; ++i) ++cnt[(src[i] >> shift) & mask];
        uint32_t sum = 0;
        for (uint32_t b = 0; b < buckets; ++b) {
            uint32_t c = cnt[b];
            cnt[b] = sum;
            sum += c;
        }
        for (size_t i = 0; i < n; ++i) dst[cnt[(src[i] >> shift) & mask]++] = src[i];
    };
    pass(data, tmp.data(), 32, 2047u, 2048u);
    pass(tmp.data(), data, 43, 2047u, 2048u);
    uint32_t hi0 = uint32_t(data[0] >> 54);
    bool need3 = false;
    for (size_t i = 1; i < n; ++i) {
        if (uint32_t(data[i] >> 54) != hi0) { need3 = true; break; }
    }
    if (need3) {
        pass(data, tmp.data(), 54, 1023u, 1024u);
        std::memcpy(data, tmp.data(), n * sizeof(uint64_t));
    }
}

// ---------------- Write campaign_daily Parquet ----------------
inline void write_campaign_daily(const fs::path& out_file,
                                 const std::vector<WorkerCampaigns>& partials,
                                 CampaignLayout layout,
                                 const Catalog& catalog) {
    struct Row {
        uint32_t camp;
        int32_t day;
        SparseCampAcc acc;
    };
    std::vector<Row> rows;
    if (layout.dense) {
        const size_t total_slots = size_t(layout.campaigns) * layout.days;
        rows.reserve(total_slots);
        for (uint32_t camp = 0; camp < layout.campaigns; ++camp) {
            const size_t row_base = size_t(camp) * layout.days;
            for (uint32_t d = 0; d < layout.days; ++d) {
                const size_t idx = row_base + d;
                SparseCampAcc sum{};
                for (const auto& p : partials) {
                    const auto& c = p.credit_dense()[idx];
                    sum.sessions += c.sessions;
                    sum.purchasing += c.purchasing;
                    sum.revenue += c.revenue;
                    sum.assists += p.assists_dense()[idx];
                }
                if (sum.sessions || sum.assists) {
                    rows.push_back({camp, layout.first_day + int32_t(d), sum});
                }
            }
        }
    } else {
        std::unordered_map<uint64_t, SparseCampAcc> merged;
        for (const auto& p : partials) {
            for (const auto& [k, a] : p.sparse()) {
                auto& s = merged[k];
                s.sessions += a.sessions;
                s.purchasing += a.purchasing;
                s.revenue += a.revenue;
                s.assists += a.assists;
            }
        }
        rows.reserve(merged.size());
        for (const auto& [k, a] : merged) {
            if (a.sessions || a.assists) rows.push_back({uint32_t(k), int32_t(k >> 32), a});
        }
    }

    pq::ParquetFileWriter w(out_file.string(), campaign_daily_schema());
    if (!rows.empty()) {
        const size_t n = rows.size();
        pq::ByteBuf camp_buf, chan_buf;
        camp_buf.reserve(n * 12);
        chan_buf.reserve(n * 12);
        std::vector<int32_t> days;
        std::vector<int64_t> sess, purch, rev, ass;
        days.reserve(n);
        sess.reserve(n);
        purch.reserve(n);
        rev.reserve(n);
        ass.reserve(n);
        for (const auto& r : rows) {
            const auto& cs = catalog.campaigns[r.camp];
            camp_buf.push_u32_le(uint32_t(cs.size()));
            camp_buf.append(cs.data(), cs.size());
            const auto& ch = catalog.channels[r.camp];
            chan_buf.push_u32_le(uint32_t(ch.size()));
            chan_buf.append(ch.data(), ch.size());
            days.push_back(r.day);
            sess.push_back(int64_t(r.acc.sessions));
            purch.push_back(int64_t(r.acc.purchasing));
            rev.push_back(int64_t(r.acc.revenue));
            ass.push_back(int64_t(r.acc.assists));
        }
        auto& rg = w.begin_row_group(int64_t(n));
        w.write_plain_column(rg, 0, camp_buf.data.data(), camp_buf.size(), int64_t(n));
        w.write_plain_column(rg, 1, chan_buf.data.data(), chan_buf.size(), int64_t(n));
        w.write_plain_column(rg, 2, days.data(), n * sizeof(int32_t), int64_t(n));
        w.write_plain_column(rg, 3, sess.data(), n * sizeof(int64_t), int64_t(n));
        w.write_plain_column(rg, 4, purch.data(), n * sizeof(int64_t), int64_t(n));
        w.write_plain_column(rg, 5, rev.data(), n * sizeof(int64_t), int64_t(n));
        w.write_plain_column(rg, 6, ass.data(), n * sizeof(int64_t), int64_t(n));
    }
    w.close();
}

// ---------------- COMPACT PIPELINE ----------------
bool run_compact(const PreadInput& ev_in, size_t ev_buckets, const fs::path& data_dir, const fs::path& out_dir,
                 unsigned threads, const Window& win, const FastTime& ft, const Catalog& catalog,
                 RawKeys<'u'>& user_keys, const People& people,
                 const pq::ByteBuf& cat_dict, CampaignLayout camp_layout, bool profile) {
    const uint32_t ev_bucket_mask = uint32_t(ev_buckets - 1);

    auto get_ru = []() -> std::pair<double, double> {
        rusage ru{};
        ::getrusage(RUSAGE_SELF, &ru);
        return {ru.ru_utime.tv_sec + ru.ru_utime.tv_usec * 1e-6,
                ru.ru_stime.tv_sec + ru.ru_stime.tv_usec * 1e-6};
    };
    auto t_mark = std::chrono::steady_clock::now();
    auto [u_mark, s_mark] = get_ru();
    auto mark = [&](const char* label) {
        if (!profile) return;
        auto now = std::chrono::steady_clock::now();
        auto [u, s] = get_ru();
        std::cerr << label << " wall=" << std::chrono::duration<double>(now - t_mark).count()
                  << "s user=" << (u - u_mark) << "s sys=" << (s - s_mark) << "s\n";
        t_mark = now;
        u_mark = u;
        s_mark = s;
    };

    const int64_t ingest_epoch_sec = (win.start / 1000LL) - 86400LL * 365LL;
    const int64_t click_start = win.start - REACH_MS;
    const int32_t first_day = int32_t(time_detail::day_number(win.start));
    const int64_t win_start_ms_of_day = win.start - int64_t(first_day) * DAY_MS;

    RawKeys<'e'> ev_keys;
    std::vector<WorkerArena> ev_arenas(threads);
    std::vector<WorkerArena> dedup_arenas(threads);

    std::vector<std::array<SlabList<CompactEv>, SHARDS>> ev_slabs(threads);
    std::vector<std::vector<SlabList<CompactDedup>>> dedup_slabs(threads);
    for (unsigned w = 0; w < threads; ++w) dedup_slabs[w].resize(ev_buckets);
    std::atomic<bool> ingest_overflow{false};

    // -------- STAGE 1: 6-Instruction Raw-ASCII Parser + 64-Row Prefetch-Pipelined Resolve --------
    {
        constexpr size_t BATCH_Q = 64;
        struct BatchEv {
            uint64_t ev_key;
            uint64_t user_key;
            uint64_t user_h;
            uint32_t ingest_off;
            uint32_t ts_off;
            uint32_t price_cat;
        };
        std::vector<std::array<BatchEv, BATCH_Q>> batch_q(threads);
        std::vector<size_t> batch_n(threads, 0);

        auto emit_resolved = [&](unsigned w, uint64_t ev_key, uint32_t ioff,
                                 uint64_t person, uint64_t person_h,
                                 uint32_t ts_off, uint32_t price_cat) __attribute__((always_inline)) {
            const uint32_t shard = uint32_t(person_h) & uint32_t(SHARDS - 1);
            const uint32_t loc = ev_slabs[w][shard].push_with_loc(
                ev_arenas[w], uint32_t(w) << 28, {person, ts_off, price_cat});
            const uint32_t eb = uint32_t(fast_hash(ev_key)) & ev_bucket_mask;
            dedup_slabs[w][eb].push(dedup_arenas[w], {ev_key, ioff, loc});
        };

        auto emit_invalid = [&](unsigned w, uint64_t ev_key, uint32_t ioff) __attribute__((always_inline)) {
            const uint32_t eb = uint32_t(fast_hash(ev_key)) & ev_bucket_mask;
            dedup_slabs[w][eb].push(dedup_arenas[w], {ev_key, ioff, INVALID_LOC});
        };

        // 4-pass batch resolve:
        // 1) Bloom prefetches were already issued during parsing -> 100% L1 hits!
        // 2) Split into misses + hits; immediately issue slots_ prefetches for ALL hits.
        // 3) Emit all ~40 misses to slabs while ~24 slots_ cache lines stream in from DRAM!
        // 4) Resolve & emit all ~24 hits at 0-cycle DRAM stall!
        auto flush_batch = [&](unsigned w) __attribute__((noinline)) {
            const size_t cnt = batch_n[w];
            if (cnt == 0) return;
            const auto* q = batch_q[w].data();
            if (__builtin_expect(!people.has_aliases(), 0)) {
                for (size_t i = 0; i < cnt; ++i) {
                    const auto& e = q[i];
                    emit_resolved(w, e.ev_key, e.ingest_off, e.user_key, e.user_h, e.ts_off, e.price_cat);
                }
                batch_n[w] = 0;
                return;
            }
            uint8_t miss_idx[BATCH_Q];
            uint8_t hit_idx[BATCH_Q];
            size_t mn = 0, hn = 0;
            for (size_t i = 0; i < cnt; ++i) {
                if (!people.check_and_prefetch_h(q[i].user_h)) {
                    miss_idx[mn++] = uint8_t(i);
                } else {
                    hit_idx[hn++] = uint8_t(i);
                }
            }
            for (size_t k = 0; k < mn; ++k) {
                const auto& e = q[miss_idx[k]];
                emit_resolved(w, e.ev_key, e.ingest_off, e.user_key, e.user_h, e.ts_off, e.price_cat);
            }
            for (size_t k = 0; k < hn; ++k) {
                const auto& e = q[hit_idx[k]];
                uint64_t person = people.lookup_hashed(e.user_key, e.user_h);
                uint64_t ph = (person == e.user_key) ? e.user_h : fast_hash(person);
                emit_resolved(w, e.ev_key, e.ingest_off, person, ph, e.ts_off, e.price_cat);
            }
            batch_n[w] = 0;
        };

        auto push_valid_ev = [&](unsigned w, uint64_t ev_key, uint32_t ioff,
                                 uint64_t user_key, uint32_t ts_off, uint32_t price_cat) __attribute__((always_inline)) {
            const uint64_t uh = fast_hash(user_key);
            people.prefetch_bloom_h(uh);
            const size_t bi = batch_n[w]++;
            batch_q[w][bi] = {ev_key, user_key, uh, ioff, ts_off, price_cat};
            if (__builtin_expect(bi + 1 == BATCH_Q, 0)) flush_batch(w);
        };

        ev_in.scan_buffers(threads, [&](unsigned w, const char* buf, size_t scan_len) {
            const char* cur = buf;
            const char* end = buf + scan_len;
            const __m128i c_lo = _mm_set1_epi8(0x30);
            const __m128i c_span = _mm_set1_epi8(74); // [0x30..0x7A] covers 0-9, A-Z, _, a-z!
            const __m256i v_nl = _mm256_set1_epi8('\n');
            const __m256i v_cm = _mm256_set1_epi8(',');
            const __m256i v_qt = _mm256_set1_epi8('"');
            std::array<SV, 7> f;

            while (cur < end) {
                const char* line_start = cur;
                const char* p = cur;

                uint64_t ev_key, user_key;
                int64_t ts, ing_sec, ioff;
                // 6-instruction raw-ASCII ID pair check (ZERO base-36 math!)
                {
                    const size_t q0 = (*p == '"');
                    const char* s0 = p + q0;
                    const char* p1 = s0 + 10 + q0;
                    const size_t q1 = (*p1 == '"');
                    const char* s1 = p1 + q1;
                    bool both9 = (s0[0] == 'e') & (s0[9] == (q0 ? '"' : ',')) & (!q0 | (s0[10] == ',')) &
                                 (s1[0] == 'u') & (s1[9] == (q1 ? '"' : ',')) & (!q1 | (s1[10] == ','));
                    if (__builtin_expect(both9, 1)) {
                        std::memcpy(&ev_key, s0 + 1, 8);
                        std::memcpy(&user_key, s1 + 1, 8);
                        __m128i v = _mm_set_epi64x(int64_t(user_key), int64_t(ev_key));
                        __m128i bad = _mm_subs_epu8(_mm_sub_epi8(v, c_lo), c_span);
                        if (__builtin_expect(_mm_testz_si128(bad, bad), 1)) {
                            p = s1 + 10 + q1;
                        } else {
                            goto slow_line;
                        }
                    } else {
                        if (s0[0] != 'e') goto slow_line;
                        size_t l0 = (s0[9] == (q0 ? '"' : ',') && (!q0 || s0[10] == ',')) ? 9 :
                                    (s0[8] == (q0 ? '"' : ',') && (!q0 || s0[9] == ',')) ? 8 : 0;
                        if (!l0) goto slow_line;
                        ev_key = 0; std::memcpy(&ev_key, s0 + 1, l0 - 1);
                        p = s0 + l0 + 1 + q0;
                        const size_t qu = (*p == '"');
                        const char* su = p + qu;
                        if (su[0] != 'u') goto slow_line;
                        size_t l1 = (su[9] == (qu ? '"' : ',') && (!qu || su[10] == ',')) ? 9 :
                                    (su[8] == (qu ? '"' : ',') && (!qu || su[9] == ',')) ? 8 : 0;
                        if (!l1) goto slow_line;
                        user_key = 0; std::memcpy(&user_key, su + 1, l1 - 1);
                        uint64_t ve_chk = (l0 == 9) ? ev_key : (ev_key | (uint64_t('0') << 56));
                        uint64_t vu_chk = (l1 == 9) ? user_key : (user_key | (uint64_t('0') << 56));
                        __m128i v = _mm_set_epi64x(int64_t(vu_chk), int64_t(ve_chk));
                        __m128i bad = _mm_subs_epu8(_mm_sub_epi8(v, c_lo), c_span);
                        if (__builtin_expect(!_mm_testz_si128(bad, bad), 0)) goto slow_line;
                        p = su + l1 + 1 + qu;
                    }
                }

                // Field 2 (ts) + Field 3 (ingest_ts): unified ISO (F1+F2 ~66%) vs F3 (~33%)!
                {
                    const size_t q2 = (*p == '"');
                    const char* s2 = p + q2;
                    if (s2[4] == '-') {
                        uint64_t pfx;
                        std::memcpy(&pfx, s2, 8);
                        int idx = ft.use_fast_months ? ft.find_month(pfx) : -1;
                        if (__builtin_expect(idx < 0, 0)) goto slow_line;
                        const auto& me = ft.months[idx];
                        uint64_t lo; uint32_t hi;
                        std::memcpy(&lo, s2 + 8, 8);
                        std::memcpy(&hi, s2 + 16, 4);
                        if (__builtin_expect(((lo & 0x0000FF0000FF0000ULL) != 0x00003A0000540000ULL) ||
                                             ((hi & 0x000000FFU) != 0x3AU), 0)) goto slow_line;
                        uint64_t d6 = ((lo & 0xFFFF00FFFF00FFFFULL) | 0x0000300000300000ULL) - 0x3030303030303030ULL;
                        uint32_t d2 = ((hi >> 8) & 0xFFFFU) - 0x3030U;
                        if (__builtin_expect((((d6 | (d6 + 0x0606060606060606ULL)) | uint64_t(d2 | (d2 + 0x0606U))) &
                                              0xF0F0F0F0F0F0F0F0ULL) != 0, 0)) goto slow_line;
                        uint32_t day = uint32_t(d6 & 0xFF) * 10u + uint32_t((d6 >> 8) & 0xFF);
                        uint32_t h   = uint32_t((d6 >> 24) & 0xFF) * 10u + uint32_t((d6 >> 32) & 0xFF);
                        uint32_t mi  = uint32_t((d6 >> 48) & 0xFF) * 10u + uint32_t((d6 >> 56) & 0xFF);
                        uint32_t sec = uint32_t(d2 & 0xFF) * 10u + uint32_t((d2 >> 8) & 0xFF);
                        if (__builtin_expect((day - 1u) >= me.max_day || h >= 24u || mi >= 60u || sec >= 60u, 0))
                            goto slow_line;
                        int64_t ts_day_sec = me.day0_sec + int64_t(day) * 86400LL;
                        int64_t ts_sec = ts_day_sec + int64_t(h * 3600u + mi * 60u + sec);
                        const char tz = s2[19];
                        if (__builtin_expect(tz == 'Z', 1)) {
                            if (__builtin_expect(!skip_delim_comma(p, s2, 20, q2), 0)) goto slow_line;
                        } else if ((tz == '+' || tz == '-') && s2[22] == ':' && skip_delim_comma(p, s2, 25, q2)) {
                            if (__builtin_expect(!time_detail::digits2(s2 + 20) || !time_detail::digits2(s2 + 23), 0))
                                goto slow_line;
                            uint32_t oh = time_detail::pair2(s2 + 20);
                            uint32_t om = time_detail::pair2(s2 + 23);
                            if (__builtin_expect(oh >= 24u || om >= 60u, 0)) goto slow_line;
                            int64_t off = int64_t(oh * 60u + om) * 60LL;
                            ts_sec += (tz == '+') ? -off : off;
                        } else {
                            goto slow_line;
                        }
                        ts = ts_sec * 1000LL;
                        if (__builtin_expect(ts < win.start || ts >= win.end, 0)) goto slow_line;

                        const size_t q3 = (*p == '"');
                        const char* s3 = p + q3;
                        if (__builtin_expect(!skip_delim_comma(p, s3, 20, q3), 0)) goto slow_line;
                        uint64_t ipfx; uint16_t iday, tday;
                        std::memcpy(&ipfx, s3, 8);
                        std::memcpy(&iday, s3 + 8, 2);
                        std::memcpy(&tday, s2 + 8, 2);
                        if (__builtin_expect(ipfx == pfx && iday == tday, 1)) {
                            uint64_t ilo; uint32_t ihi;
                            std::memcpy(&ilo, s3 + 8, 8);
                            std::memcpy(&ihi, s3 + 16, 4);
                            if (__builtin_expect(((ilo & 0x0000FF0000FF0000ULL) != 0x00003A0000540000ULL) ||
                                                 ((ihi & 0xFF0000FFU) != 0x5A00003AU), 0)) goto slow_line;
                            uint64_t id6 = ((ilo & 0xFFFF00FFFF000000ULL) | 0x0000300000303030ULL) - 0x3030303030303030ULL;
                            uint32_t id2 = ((ihi >> 8) & 0xFFFFU) - 0x3030U;
                            if (__builtin_expect((((id6 | (id6 + 0x0606060606060606ULL)) | uint64_t(id2 | (id2 + 0x0606U))) &
                                                  0xF0F0F0F0F0F0F0F0ULL) != 0, 0)) goto slow_line;
                            uint32_t ih = uint32_t((id6 >> 24) & 0xFF) * 10u + uint32_t((id6 >> 32) & 0xFF);
                            uint32_t im = uint32_t((id6 >> 48) & 0xFF) * 10u + uint32_t((id6 >> 56) & 0xFF);
                            uint32_t is = uint32_t(id2 & 0xFF) * 10u + uint32_t((id2 >> 8) & 0xFF);
                            if (__builtin_expect(ih >= 24u || im >= 60u || is >= 60u, 0)) goto slow_line;
                            ing_sec = ts_day_sec + int64_t(ih * 3600u + im * 60u + is);
                        } else {
                            int64_t dms = 0;
                            ing_sec = ft.parse_ingest_sec(SV(s3, 20), dms);
                        }
                        if (__builtin_expect(ing_sec == INVALID_TS || ing_sec - ts_sec > 86400LL, 0)) goto slow_line;
                    } else {
                        if (__builtin_expect(!skip_delim_comma(p, s2, 13, q2), 0)) goto slow_line;
                        ts = time_detail::parse_f3_13(s2);
                        if (__builtin_expect(ts < win.start || ts >= win.end, 0)) goto slow_line;
                        const size_t q3 = (*p == '"');
                        const char* s3 = p + q3;
                        if (__builtin_expect(!skip_delim_comma(p, s3, 20, q3), 0)) goto slow_line;
                        int64_t dms = 0;
                        ing_sec = ft.parse_ingest_sec(SV(s3, 20), dms);
                        if (__builtin_expect(ing_sec == INVALID_TS || ing_sec * 1000LL - ts > DAY_MS, 0)) goto slow_line;
                    }
                }
                ioff = ing_sec - ingest_epoch_sec;
                if (__builtin_expect(uint64_t(ioff) >= (uint64_t{1} << 32), 0)) {
                    ingest_overflow.store(true, std::memory_order_relaxed);
                    return;
                }

                // Field 4: event_type
                int et;
                {
                    const size_t q = (*p == '"');
                    const char* s = p + q;
                    uint32_t w4;
                    std::memcpy(&w4, s, 4);
                    if (__builtin_expect((w4 == 0x77656976u || w4 == 0x74726163u) && skip_delim_comma(p, s, 4, q), 1)) {
                        et = 1;
                    } else {
                        uint64_t w8;
                        std::memcpy(&w8, s, 8);
                        if (__builtin_expect(w8 == 0x6573616863727570ULL && skip_delim_comma(p, s, 8, q), 1)) {
                            et = 2;
                        } else {
                            goto slow_line;
                        }
                    }
                }

                // Fields 5 & 6: 32-byte AVX2 tail check with unquoted fast-path!
                {
                    const __m256i tail = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
                    const uint32_t m_nl = uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(tail, v_nl)));
                    if (__builtin_expect(m_nl == 0, 0)) goto slow_line;
                    const uint32_t nl_pos = uint32_t(__builtin_ctz(m_nl));
                    const uint32_t mask = (1u << nl_pos) - 1u;
                    const uint32_t m_cm = uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(tail, v_cm))) & mask;
                    if (__builtin_expect(__builtin_popcount(m_cm) != 1, 0)) goto slow_line;
                    const uint32_t cm_pos = uint32_t(__builtin_ctz(m_cm));
                    const uint32_t m_qt = uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(tail, v_qt))) & mask;
                    uint32_t price_cat = 0;
                    if (__builtin_expect(m_qt == 0, 1)) {
                        if (__builtin_expect(cm_pos == 0, 0)) goto slow_line;
                        if (__builtin_expect(et == 2, 0)) {
                            int32_t pr = parse_price(SV(p + cm_pos + 1, nl_pos - cm_pos - 1));
                            if (__builtin_expect(pr == 0, 0)) goto slow_line;
                            price_cat = (catalog.category(SV(p, cm_pos)) << 24) | uint32_t(pr);
                        }
                        push_valid_ev(w, ev_key, uint32_t(ioff), user_key, uint32_t(ts - win.start), price_cat);
                        cur = p + nl_pos + 1;
                        continue;
                    }
                    const uint32_t starts = (m_cm << 1) | 1u;
                    const uint32_t ends = (m_cm >> 1) | (1u << (nl_pos - 1));
                    if (__builtin_expect(((m_qt & ~(starts | ends)) | (m_qt & starts & ends)) != 0, 0))
                        goto slow_line;
                    const size_t q5 = (cm_pos != 0) & size_t(p[0] == '"');
                    const size_t q5e = (cm_pos != 0) & size_t(p[cm_pos ? cm_pos - 1 : 0] == '"');
                    const size_t len6 = nl_pos - cm_pos - 1;
                    const size_t q6 = (len6 != 0) & size_t(p[cm_pos + 1] == '"');
                    const size_t q6e = (len6 != 0) & size_t(p[len6 ? nl_pos - 1 : cm_pos + 1] == '"');
                    if (__builtin_expect((q5 ^ q5e) | (q6 ^ q6e), 0)) goto slow_line;
                    const size_t prod_len = cm_pos - 2 * q5;
                    if (__builtin_expect(prod_len == 0, 0)) goto slow_line;
                    if (__builtin_expect(et == 2, 0)) {
                        int32_t pr = parse_price(SV(p + cm_pos + 1 + q6, len6 - 2 * q6));
                        if (__builtin_expect(pr == 0, 0)) goto slow_line;
                        price_cat = (catalog.category(SV(p + q5, prod_len)) << 24) | uint32_t(pr);
                    }
                    push_valid_ev(w, ev_key, uint32_t(ioff), user_key, uint32_t(ts - win.start), price_cat);
                    cur = p + nl_pos + 1;
                    continue;
                }

            slow_line:
                const char* nl = static_cast<const char*>(std::memchr(line_start, '\n', size_t(end - line_start)));
                size_t llen = nl ? size_t(nl - line_start) : size_t(end - line_start);
                cur = nl ? (nl + 1) : end;
                if (!split_fields_slow<7>(SV(line_start, llen), f) || f[0].empty()) continue;
                int64_t dms = 0;
                int64_t ing_s = ft.parse_ingest_sec(f[3], dms);
                if (ing_s == INVALID_TS) {
                    ingest_overflow.store(true, std::memory_order_relaxed);
                    return;
                }
                int64_t ioff_s = ing_s - ingest_epoch_sec;
                if (uint64_t(ioff_s) >= (uint64_t{1} << 32)) {
                    ingest_overflow.store(true, std::memory_order_relaxed);
                    return;
                }
                uint64_t ek_v = ev_keys.encode(f[0]);
                int et_v = parse_event_type(f[4]);
                if (et_v != 0 && !f[1].empty() && !f[5].empty()) {
                    int64_t ts_v = ft.parse_ts_with_ingest(f[2], f[3], dms);
                    if (ts_v >= win.start && ts_v < win.end && ing_s * 1000LL - ts_v <= DAY_MS) {
                        uint32_t pc_v = 0;
                        bool ok = true;
                        if (et_v == 2) {
                            int32_t pr = parse_price(f[6]);
                            if (pr <= 0) ok = false;
                            else pc_v = (catalog.category(f[5]) << 24) | uint32_t(pr);
                        }
                        if (ok) {
                            push_valid_ev(w, ek_v, uint32_t(ioff_s), user_keys.encode(f[1]),
                                          uint32_t(ts_v - win.start), pc_v);
                            continue;
                        }
                    }
                }
                emit_invalid(w, ek_v, uint32_t(ioff_s));
            }
        }, [&](unsigned w) {
            flush_batch(w);
            for (auto& sl : ev_slabs[w]) sl.finalize();
            for (auto& sl : dedup_slabs[w]) sl.finalize();
        });
    }
    if (ingest_overflow.load(std::memory_order_relaxed)) return false;
    mark("event parse + resolve");

    // -------- STAGE 2: Parallel single-pass AoS dedup (1 cache miss/probe + wyhash64>>32) --------
    {
        struct DedupSlot {
            uint64_t key;
            uint64_t val; // (ingest_off << 32) | loc
        };
        std::vector<std::vector<DedupSlot>> w_ht(threads);

        parallel_for(ev_buckets, threads, [&](unsigned wid, size_t b) {
            size_t total = 0;
            for (unsigned w = 0; w < threads; ++w) total += dedup_slabs[w][b].total_count();
            if (total <= 1) return;

            const uint32_t cap = std::max<uint32_t>(16u, uint32_t((total * 3) / 2) + 1u);
            auto& ht = w_ht[wid];
            if (ht.size() < cap) ht.resize(cap);
            std::memset(ht.data(), 0, size_t(cap) * sizeof(DedupSlot));
            DedupSlot* ptr = ht.data();

            auto mark_dead = [&](uint32_t loc) __attribute__((always_inline)) {
                reinterpret_cast<CompactEv*>(ev_arenas[loc >> 28].slab_ptr((loc >> 10) & 0x3FFFFu))[loc & 1023u].ts_off =
                    DEAD_TS_U32;
            };

            auto slot_of = [cap](uint64_t k) __attribute__((always_inline)) -> uint32_t {
                uint32_t h32 = uint32_t(fast_hash(k) >> 32);
                return uint32_t((uint64_t(h32) * uint64_t(cap)) >> 32);
            };

            for (unsigned w = 0; w < threads; ++w) {
                for (const auto& slab : dedup_slabs[w][b].slabs()) {
                    const CompactDedup* rows = slab.ptr;
                    const uint32_t n = slab.count;
                    for (uint32_t i = 0; i < n; ++i) {
                        if (i + 16 < n) {
                            _mm_prefetch(reinterpret_cast<const char*>(ptr + slot_of(rows[i + 16].ev_key)), _MM_HINT_T0);
                        }
                        const uint64_t ek = rows[i].ev_key;
                        const uint32_t ing = rows[i].ingest_off;
                        const uint32_t loc = rows[i].loc;
                        uint32_t slot = slot_of(ek);
                        for (;;) {
                            const uint64_t cur = ptr[slot].key;
                            if (__builtin_expect(cur == 0, 1)) {
                                ptr[slot].key = ek;
                                ptr[slot].val = (uint64_t(ing) << 32) | loc;
                                break;
                            }
                            if (__builtin_expect(cur == ek, 1)) {
                                const uint64_t cv = ptr[slot].val;
                                const uint32_t cur_ing = uint32_t(cv >> 32);
                                if (ing < cur_ing) {
                                    uint32_t old_loc = uint32_t(cv);
                                    if (old_loc != INVALID_LOC) mark_dead(old_loc);
                                    ptr[slot].val = (uint64_t(ing) << 32) | loc;
                                } else {
                                    if (loc != INVALID_LOC) mark_dead(loc);
                                }
                                break;
                            }
                            if (++slot == cap) slot = 0;
                        }
                    }
                }
            }
        });
    }
    mark("event dedup");

    // -------- STAGE 3: Raw-ASCII Fused Click Parser + 64-Row Prefetch-Pipelined Resolve --------
    for (unsigned w = 0; w < threads; ++w) dedup_arenas[w].reset();
    ClickKeys click_keys;
    std::vector<std::array<SlabList<CompactClick>, SHARDS>> click_slabs(threads);
    {
        PreadInput cl_in(data_dir / "clicks", 2u << 20);
        constexpr size_t BATCH_Q = 64;
        struct BatchClick {
            uint64_t user_key;
            uint64_t user_h;
            uint64_t click_id;
            uint32_t ts_off;
            uint32_t campaign;
        };
        std::vector<std::array<BatchClick, BATCH_Q>> batch_q(threads);
        std::vector<size_t> batch_n(threads, 0);

        auto emit_click = [&](unsigned w, uint64_t person, uint64_t ph,
                              uint64_t cid, uint32_t ts_off, uint32_t camp) __attribute__((always_inline)) {
            const uint32_t shard = uint32_t(ph) & uint32_t(SHARDS - 1);
            click_slabs[w][shard].push(dedup_arenas[w], {person, cid, ts_off, camp});
        };

        auto flush_batch = [&](unsigned w) __attribute__((noinline)) {
            const size_t cnt = batch_n[w];
            if (cnt == 0) return;
            const auto* q = batch_q[w].data();
            if (__builtin_expect(!people.has_aliases(), 0)) {
                for (size_t i = 0; i < cnt; ++i) {
                    const auto& c = q[i];
                    emit_click(w, c.user_key, c.user_h, c.click_id, c.ts_off, c.campaign);
                }
                batch_n[w] = 0;
                return;
            }
            uint8_t miss_idx[BATCH_Q];
            uint8_t hit_idx[BATCH_Q];
            size_t mn = 0, hn = 0;
            for (size_t i = 0; i < cnt; ++i) {
                if (!people.check_and_prefetch_h(q[i].user_h)) {
                    miss_idx[mn++] = uint8_t(i);
                } else {
                    hit_idx[hn++] = uint8_t(i);
                }
            }
            for (size_t k = 0; k < mn; ++k) {
                const auto& c = q[miss_idx[k]];
                emit_click(w, c.user_key, c.user_h, c.click_id, c.ts_off, c.campaign);
            }
            for (size_t k = 0; k < hn; ++k) {
                const auto& c = q[hit_idx[k]];
                uint64_t person = people.lookup_hashed(c.user_key, c.user_h);
                uint64_t ph = (person == c.user_key) ? c.user_h : fast_hash(person);
                emit_click(w, person, ph, c.click_id, c.ts_off, c.campaign);
            }
            batch_n[w] = 0;
        };

        auto push_valid_click = [&](unsigned w, uint64_t ukey, uint64_t cid,
                                    uint32_t ts_off, uint32_t camp) __attribute__((always_inline)) {
            const uint64_t uh = fast_hash(ukey);
            people.prefetch_bloom_h(uh);
            const size_t bi = batch_n[w]++;
            batch_q[w][bi] = {ukey, uh, cid, ts_off, camp};
            if (__builtin_expect(bi + 1 == BATCH_Q, 0)) flush_batch(w);
        };

        const bool fast_camp = catalog.is_fast_campaigns();

        cl_in.scan_buffers(threads, [&](unsigned w, const char* buf, size_t scan_len) {
            const char* cur = buf;
            const char* end = buf + scan_len;
            const __m128i c_lo = _mm_set1_epi8(0x30);
            const __m128i c_span = _mm_set1_epi8(74);
            std::array<SV, 4> f;

            while (cur < end) {
                const char* line_start = cur;
                const char* p = cur;

                uint64_t cid, ukey;
                // 6-instruction dual raw-ASCII check for 9-char click_id + 9-char user_id!
                {
                    const size_t q0 = (*p == '"');
                    const char* s0 = p + q0;
                    const char* p1 = s0 + 10 + q0;
                    const size_t q1 = (*p1 == '"');
                    const char* s1 = p1 + q1;
                    bool both9 = (s0[0] == 'k') & (s0[9] == (q0 ? '"' : ',')) & (!q0 | (s0[10] == ',')) &
                                 (s1[0] == 'u') & (s1[9] == (q1 ? '"' : ',')) & (!q1 | (s1[10] == ','));
                    if (__builtin_expect(both9, 1)) {
                        uint64_t raw_k;
                        std::memcpy(&raw_k, s0 + 1, 8);
                        std::memcpy(&ukey, s1 + 1, 8);
                        __m128i v = _mm_set_epi64x(int64_t(ukey), int64_t(raw_k));
                        __m128i bad = _mm_subs_epu8(_mm_sub_epi8(v, c_lo), c_span);
                        if (__builtin_expect(_mm_testz_si128(bad, bad), 1)) {
                            cid = __builtin_bswap64(raw_k);
                            p = s1 + 10 + q1;
                        } else {
                            goto slow_click;
                        }
                    } else {
                        if (s0[0] != 'k') goto slow_click;
                        size_t l0 = (s0[9] == (q0 ? '"' : ',') && (!q0 || s0[10] == ',')) ? 9 :
                                    (s0[8] == (q0 ? '"' : ',') && (!q0 || s0[9] == ',')) ? 8 : 0;
                        if (!l0) goto slow_click;
                        uint64_t raw_k = 0; std::memcpy(&raw_k, s0 + 1, l0 - 1);
                        p = s0 + l0 + 1 + q0;
                        const size_t qu = (*p == '"');
                        const char* su = p + qu;
                        if (su[0] != 'u') goto slow_click;
                        size_t l1 = (su[9] == (qu ? '"' : ',') && (!qu || su[10] == ',')) ? 9 :
                                    (su[8] == (qu ? '"' : ',') && (!qu || su[9] == ',')) ? 8 : 0;
                        if (!l1) goto slow_click;
                        ukey = 0; std::memcpy(&ukey, su + 1, l1 - 1);
                        uint64_t vk_chk = (l0 == 9) ? raw_k : (raw_k | (uint64_t('0') << 56));
                        uint64_t vu_chk = (l1 == 9) ? ukey : (ukey | (uint64_t('0') << 56));
                        __m128i v = _mm_set_epi64x(int64_t(vu_chk), int64_t(vk_chk));
                        __m128i bad = _mm_subs_epu8(_mm_sub_epi8(v, c_lo), c_span);
                        if (__builtin_expect(!_mm_testz_si128(bad, bad), 0)) goto slow_click;
                        cid = __builtin_bswap64(raw_k);
                        p = su + l1 + 1 + qu;
                    }
                }

                uint32_t camp;
                {
                    if (__builtin_expect(!fast_camp, 0)) goto slow_click;
                    const size_t q = (*p == '"');
                    const char* s = p + q;
                    if (__builtin_expect(s[0] != 'c' || s[1] != '_', 0)) goto slow_click;
                    unsigned d0 = unsigned(s[2] - '0');
                    if (__builtin_expect(d0 >= 10u || d0 == 0u, 0)) goto slow_click;
                    uint32_t num = d0;
                    size_t pos = 3;
                    while (pos < 8) {
                        unsigned d = unsigned(s[pos] - '0');
                        if (d >= 10u) break;
                        num = num * 10u + d;
                        ++pos;
                    }
                    if (__builtin_expect(!skip_delim_comma(p, s, pos, q), 0)) goto slow_click;
                    camp = catalog.campaign_by_num(num);
                    if (__builtin_expect(camp == 0, 0)) goto slow_click;
                }

                int64_t ts;
                {
                    const size_t q = (*p == '"');
                    const char* s = p + q;
                    if (s[19] == 'Z' && skip_delim_nl(p, s, 20, q)) {
                        ts = ft.parse_ts(SV(s, 20));
                    } else if (skip_delim_nl(p, s, 13, q)) {
                        ts = time_detail::parse_f3_13(s);
                    } else if (skip_delim_nl(p, s, 25, q)) {
                        ts = ft.parse_ts(SV(s, 25));
                    } else {
                        goto slow_click;
                    }
                    if (__builtin_expect(ts < click_start || ts >= win.end, 0)) {
                        cur = p;
                        continue;
                    }
                }

                push_valid_click(w, ukey, cid, uint32_t(ts - click_start), camp);
                cur = p;
                continue;

            slow_click:
                const char* nl = static_cast<const char*>(std::memchr(line_start, '\n', size_t(end - line_start)));
                size_t llen = nl ? size_t(nl - line_start) : size_t(end - line_start);
                cur = nl ? (nl + 1) : end;
                if (!split_fields_slow<4>(SV(line_start, llen), f) || f[0].empty() || f[1].empty()) continue;
                uint32_t camp_s = catalog.campaign(f[2]);
                if (camp_s == 0) continue;
                int64_t ts_s = ft.parse_ts(f[3]);
                if (ts_s < click_start || ts_s >= win.end) continue;
                push_valid_click(w, user_keys.encode(f[1]), click_keys.encode(f[0]),
                                 uint32_t(ts_s - click_start), camp_s);
            }
        }, [&](unsigned w) {
            flush_batch(w);
            for (auto& sl : click_slabs[w]) sl.finalize();
        });
    }
    mark("click parse + resolve");

    // -------- STAGE 4: LPT Shard Schedule -> 64 Coarse -> L1-Hot HT -> 8B Sort -> Transposed Campaigns --------
    fs::create_directories(out_dir / "user_daily");
    fs::create_directories(out_dir / "campaign_daily");

    std::array<std::pair<size_t, size_t>, SHARDS> shard_order;
    for (size_t s = 0; s < SHARDS; ++s) {
        size_t cnt = 0;
        for (unsigned w = 0; w < threads; ++w) cnt += ev_slabs[w][s].total_count();
        shard_order[s] = {cnt, s};
    }
    std::sort(shard_order.begin(), shard_order.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });

    std::vector<WorkerCampaigns> worker_camps;
    worker_camps.reserve(threads);
    for (unsigned w = 0; w < threads; ++w) worker_camps.emplace_back(camp_layout);

    std::atomic<size_t> next_shard{0};
    std::atomic<bool> any_users{false};
    std::mutex err_mu;
    std::exception_ptr err;

    auto worker_fn = [&](unsigned wid) {
        try {
            UserParquetStream pq_out((out_dir / "user_daily" / ("part-" + std::to_string(wid) + ".parquet")).string(),
                                     cat_dict, int32_t(catalog.categories.size()),
                                     camp_layout.first_day, camp_layout.days);
            auto& camp_totals = worker_camps[wid];

            constexpr size_t L1B = 64;
            std::array<std::vector<CompactEv>, L1B> ev_b;
            std::array<std::vector<CompactClick>, L1B> cl_b;
            const size_t init_ev_res = shard_order[0].first / 64 + 64;
            for (size_t b = 0; b < L1B; ++b) {
                ev_b[b].reserve(init_ev_res);
                cl_b[b].reserve(64);
            }

            // L1-hot per-coarse-bucket SoA hash table (512 slots = 6 KiB in L1d!)
            std::vector<uint64_t> ht_k(512, 0);
            std::vector<uint32_t> ht_pid(512, 0);
            std::vector<uint32_t> ht_used;
            ht_used.reserve(512);
            std::vector<uint64_t> p_keys;
            std::vector<uint32_t> p_ev_cnt, p_ev_pos;
            std::vector<uint32_t> p_cl_cnt, p_cl_pos;
            std::vector<uint32_t> ev_pid;
            std::vector<uint32_t> cl_pid, cl_src_idx;
            std::vector<uint64_t> ev_packed, ev_radix_tmp;
            std::vector<PackedClick> cl_packed;

            std::vector<SessionRec> sessions;
            std::vector<TempUserDay> udays;
            std::vector<CoverageRec> covered(catalog.campaigns.size(), {0, 0});
            DailyCats daily_cats(catalog.categories.size());
            uint32_t user_gen = 0;
            char ustr_buf[9];

            auto grow_ht = [&]() {
                size_t ncap = ht_k.size() * 2;
                ht_k.assign(ncap, 0);
                ht_pid.resize(ncap);
                ht_used.clear();
                uint32_t mask = uint32_t(ncap - 1);
                for (uint32_t pid = 0; pid < p_keys.size(); ++pid) {
                    uint64_t pk = p_keys[pid];
                    uint32_t slot = uint32_t(fast_hash(pk) >> (SHARD_BITS + 6)) & mask;
                    while (ht_k[slot] != 0) slot = (slot + 1) & mask;
                    ht_k[slot] = pk;
                    ht_pid[slot] = pid;
                    ht_used.push_back(slot);
                }
            };

            for (;;) {
                size_t order_idx = next_shard.fetch_add(1, std::memory_order_relaxed);
                if (order_idx >= SHARDS) break;
                const size_t shard = shard_order[order_idx].second;
                if (shard_order[order_idx].first == 0) continue;

                for (size_t b = 0; b < L1B; ++b) {
                    ev_b[b].clear();
                    cl_b[b].clear();
                }

                // Single-pass Level 1 events -> 64 L1-DTLB-resident coarse buckets!
                bool any_live = false;
                for (unsigned w = 0; w < threads; ++w) {
                    for (const auto& slab : ev_slabs[w][shard].slabs()) {
                        const CompactEv* p = slab.ptr;
                        const uint32_t n = slab.count;
                        for (uint32_t i = 0; i < n; ++i) {
                            if (__builtin_expect(p[i].ts_off == DEAD_TS_U32, 0)) continue;
                            ev_b[(fast_hash(p[i].person) >> SHARD_BITS) & 63u].push_back(p[i]);
                            any_live = true;
                        }
                    }
                }
                if (!any_live) continue;

                // Single-pass Level 1 clicks -> 64 coarse buckets (skip empty event buckets!)
                for (unsigned w = 0; w < threads; ++w) {
                    for (const auto& slab : click_slabs[w][shard].slabs()) {
                        const CompactClick* p = slab.ptr;
                        const uint32_t n = slab.count;
                        for (uint32_t i = 0; i < n; ++i) {
                            uint32_t cb = uint32_t(fast_hash(p[i].person) >> SHARD_BITS) & 63u;
                            if (ev_b[cb].empty()) continue;
                            cl_b[cb].push_back(p[i]);
                        }
                    }
                }

                // Process each L2-hot coarse bucket via L1-hot hash table!
                for (size_t cb = 0; cb < L1B; ++cb) {
                    const uint32_t cen = uint32_t(ev_b[cb].size());
                    if (cen == 0) continue;
                    const uint32_t ccn = uint32_t(cl_b[cb].size());

                    for (uint32_t slot : ht_used) ht_k[slot] = 0;
                    ht_used.clear();
                    p_keys.clear();
                    p_ev_cnt.clear();

                    const CompactEv* ce_ptr = ev_b[cb].data();
                    ev_pid.resize(cen);
                    uint32_t ht_mask = uint32_t(ht_k.size() - 1);
                    uint64_t* hk = ht_k.data();
                    uint32_t* hp = ht_pid.data();

                    for (uint32_t i = 0; i < cen; ++i) {
                        const uint64_t pk = ce_ptr[i].person;
                        uint32_t slot = uint32_t(fast_hash(pk) >> (SHARD_BITS + 6)) & ht_mask;
                        uint32_t pid;
                        for (;;) {
                            uint64_t cur = hk[slot];
                            if (cur == pk) {
                                pid = hp[slot];
                                break;
                            }
                            if (cur == 0) {
                                pid = uint32_t(p_keys.size());
                                p_keys.push_back(pk);
                                p_ev_cnt.push_back(0);
                                hk[slot] = pk;
                                hp[slot] = pid;
                                ht_used.push_back(slot);
                                if (__builtin_expect(p_keys.size() * 10 >= ht_k.size() * 7, 0)) {
                                    grow_ht();
                                    ht_mask = uint32_t(ht_k.size() - 1);
                                    hk = ht_k.data();
                                    hp = ht_pid.data();
                                }
                                break;
                            }
                            slot = (slot + 1) & ht_mask;
                        }
                        ev_pid[i] = pid;
                        ++p_ev_cnt[pid];
                    }

                    const uint32_t U = uint32_t(p_keys.size());
                    p_ev_pos.resize(U);
                    uint32_t esum = 0;
                    for (uint32_t u = 0; u < U; ++u) {
                        p_ev_pos[u] = esum;
                        esum += p_ev_cnt[u];
                        p_ev_cnt[u] = esum;
                    }

                    ev_packed.resize(cen);
                    uint64_t* ep_base = ev_packed.data();
                    for (uint32_t i = 0; i < cen; ++i) {
                        ep_base[p_ev_pos[ev_pid[i]]++] =
                            (uint64_t(ce_ptr[i].ts_off) << 32) | uint64_t(ce_ptr[i].price_cat);
                    }

                    p_cl_cnt.assign(U, 0);
                    if (ccn > 0) {
                        const CompactClick* cc_ptr = cl_b[cb].data();
                        cl_pid.clear();
                        cl_src_idx.clear();
                        for (uint32_t i = 0; i < ccn; ++i) {
                            const uint64_t pk = cc_ptr[i].person;
                            uint32_t slot = uint32_t(fast_hash(pk) >> (SHARD_BITS + 6)) & ht_mask;
                            for (;;) {
                                uint64_t cur = hk[slot];
                                if (cur == pk) {
                                    uint32_t pid = hp[slot];
                                    cl_pid.push_back(pid);
                                    cl_src_idx.push_back(i);
                                    ++p_cl_cnt[pid];
                                    break;
                                }
                                if (cur == 0) break;
                                slot = (slot + 1) & ht_mask;
                            }
                        }
                        uint32_t csum = 0;
                        p_cl_pos.resize(U);
                        for (uint32_t u = 0; u < U; ++u) {
                            p_cl_pos[u] = csum;
                            csum += p_cl_cnt[u];
                            p_cl_cnt[u] = csum;
                        }
                        cl_packed.resize(csum);
                        PackedClick* cp_base = cl_packed.data();
                        for (size_t k = 0; k < cl_pid.size(); ++k) {
                            const auto& c = cc_ptr[cl_src_idx[k]];
                            cp_base[p_cl_pos[cl_pid[k]]++] = {c.click_id, c.ts_off, c.campaign};
                        }
                    }

                    const bool camp_dense = camp_totals.is_dense();
                    uint32_t e_lo = 0, c_lo = 0;
                    for (uint32_t u = 0; u < U; ++u) {
                        const uint32_t e_hi = p_ev_cnt[u];
                        const uint32_t c_hi = p_cl_cnt[u];
                        const size_t en = e_hi - e_lo;
                        uint64_t* ep = ev_packed.data() + e_lo;

                        sort_packed_events(ep, en, ev_radix_tmp);

                        sessions.clear();
                        udays.clear();

                        uint32_t first_ts = uint32_t(ep[0] >> 32);
                        int32_t cur_day_idx = int32_t((uint64_t(first_ts) + uint64_t(win_start_ms_of_day)) / uint64_t(DAY_MS));
                        int32_t cur_day = first_day + cur_day_idx;
                        uint64_t next_day_ts = uint64_t(cur_day_idx + 1) * uint64_t(DAY_MS) - uint64_t(win_start_ms_of_day);

                        uint32_t first_sess_of_day = 0;
                        int32_t day_sess = 0, day_evs = 0;
                        int64_t day_rev = 0;

                        auto close_day = [&]() __attribute__((always_inline)) {
                            int32_t top = daily_cats.finish_day();
                            udays.push_back({cur_day, day_sess, day_evs, day_rev, top});
                            const uint32_t se = uint32_t(sessions.size());
                            while (first_sess_of_day < se) sessions[first_sess_of_day++].day_end = se;
                        };

                        size_t ei = 0;
                        while (ei < en) {
                            const size_t s_start_idx = ei;
                            const uint32_t s_start_off = uint32_t(ep[ei] >> 32);
                            if (__builtin_expect(s_start_off >= next_day_ts, 0)) {
                                close_day();
                                cur_day_idx = int32_t((uint64_t(s_start_off) + uint64_t(win_start_ms_of_day)) / uint64_t(DAY_MS));
                                cur_day = first_day + cur_day_idx;
                                next_day_ts = uint64_t(cur_day_idx + 1) * uint64_t(DAY_MS) - uint64_t(win_start_ms_of_day);
                                day_sess = 0; day_evs = 0; day_rev = 0;
                            }
                            int64_t s_rev = 0;
                            uint32_t prev_ts = s_start_off;
                            for (;;) {
                                const uint32_t pc = uint32_t(ep[ei]);
                                if (__builtin_expect(pc != 0, 0)) {
                                    uint32_t pr = pc & 0x00FFFFFFu;
                                    uint32_t cat = pc >> 24;
                                    s_rev += pr;
                                    daily_cats.add(cat, pr);
                                }
                                ++ei;
                                if (ei >= en) break;
                                uint32_t nxt_ts = uint32_t(ep[ei] >> 32);
                                if (nxt_ts - prev_ts > uint32_t(GAP_MS)) break;
                                prev_ts = nxt_ts;
                            }
                            ++day_sess;
                            day_evs += int32_t(ei - s_start_idx);
                            day_rev += s_rev;
                            sessions.push_back({int64_t(s_start_off) + REACH_MS, s_rev, cur_day, 0});
                        }
                        close_day();

                        SV ustr = user_keys.decode(p_keys[u], ustr_buf);
                        pq_out.begin_user(ustr, uint32_t(udays.size()));
                        for (const auto& ud : udays) {
                            pq_out.add_day(ud.day, ud.sessions, ud.events, ud.revenue, ud.top_cat);
                        }

                        const size_t cn = c_hi - c_lo;
                        if (cn == 0) {
                            for (const auto& s : sessions) camp_totals.add_organic(s.day, s.revenue);
                        } else if (cn == 1) {
                            // Fast single-click path: zero sort, zero `covered[]` traffic!
                            const PackedClick& cl = cl_packed[c_lo];
                            const int64_t cl_ts = int64_t(cl.ts_off);
                            size_t first = 0;
                            while (first < sessions.size() && sessions[first].start < cl_ts) {
                                const auto& s = sessions[first++];
                                camp_totals.add_organic(s.day, s.revenue);
                            }
                            size_t end_i = first;
                            while (end_i < sessions.size() && (sessions[end_i].start - cl_ts) <= REACH_MS) {
                                ++end_i;
                            }
                            if (first < end_i) {
                                if (__builtin_expect(camp_dense, 1)) {
                                    CreditAcc* crow = camp_totals.credit_row(cl.campaign);
                                    uint32_t* arow = camp_totals.assists_row(cl.campaign);
                                    for (size_t pos = first; pos < end_i; ++pos) {
                                        const SessionRec& s = sessions[pos];
                                        const size_t d = size_t(s.day - first_day);
                                        ++crow[d].sessions;
                                        crow[d].purchasing += (s.revenue > 0);
                                        crow[d].revenue += uint64_t(s.revenue);
                                        ++arow[d];
                                    }
                                } else {
                                    for (size_t pos = first; pos < end_i; ++pos) {
                                        const SessionRec& s = sessions[pos];
                                        camp_totals.add_credit(cl.campaign, s.day, s.revenue);
                                        camp_totals.add_assists(cl.campaign, s.day, 1u);
                                    }
                                }
                            }
                            while (end_i < sessions.size()) {
                                const auto& s = sessions[end_i++];
                                camp_totals.add_organic(s.day, s.revenue);
                            }
                        } else {
                            PackedClick* cp = cl_packed.data() + c_lo;
                            sort_detail::sort_packed_clicks(cp, cn);

                            ++user_gen;
                            if (__builtin_expect(user_gen == 0, 0)) {
                                std::fill(covered.begin(), covered.end(), CoverageRec{0, 0});
                                user_gen = 1;
                            }
                            size_t first = 0, end_i = 0;
                            const PackedClick* latest = nullptr;
                            CreditAcc* org_row = camp_dense ? camp_totals.credit_row(0u) : nullptr;
                            CreditAcc* latest_row = org_row;
                            auto credit = [&](const SessionRec& s) __attribute__((always_inline)) {
                                if (__builtin_expect(camp_dense, 1)) {
                                    CreditAcc* row = (latest && (s.start - int64_t(latest->ts_off)) <= REACH_MS)
                                                         ? latest_row
                                                         : org_row;
                                    auto& acc = row[size_t(s.day - first_day)];
                                    ++acc.sessions;
                                    acc.purchasing += (s.revenue > 0);
                                    acc.revenue += uint64_t(s.revenue);
                                    return;
                                }
                                const uint32_t camp = (latest && (s.start - int64_t(latest->ts_off)) <= REACH_MS)
                                                          ? latest->campaign
                                                          : 0u;
                                camp_totals.add_credit(camp, s.day, s.revenue);
                            };

                            for (size_t ci = 0; ci < cn; ++ci) {
                                const PackedClick& cl = cp[ci];
                                const int64_t cl_ts = int64_t(cl.ts_off);
                                while (first < sessions.size() && sessions[first].start < cl_ts) {
                                    credit(sessions[first++]);
                                }
                                if (end_i < first) end_i = first;
                                while (end_i < sessions.size() && (sessions[end_i].start - cl_ts) <= REACH_MS) {
                                    ++end_i;
                                }
                                if (first < end_i) {
                                    CoverageRec& prior = covered[cl.campaign];
                                    size_t pos = (prior.gen == user_gen) ? std::max(first, size_t(prior.end)) : first;
                                    if (__builtin_expect(camp_dense, 1)) {
                                        uint32_t* arow = camp_totals.assists_row(cl.campaign);
                                        while (pos < end_i) {
                                            const SessionRec& s = sessions[pos];
                                            size_t nxt = std::min(end_i, size_t(s.day_end));
                                            arow[s.day - first_day] += uint32_t(nxt - pos);
                                            pos = nxt;
                                        }
                                    } else {
                                        while (pos < end_i) {
                                            const SessionRec& s = sessions[pos];
                                            size_t nxt = std::min(end_i, size_t(s.day_end));
                                            camp_totals.add_assists(cl.campaign, s.day, uint32_t(nxt - pos));
                                            pos = nxt;
                                        }
                                    }
                                    prior = {user_gen, uint32_t(end_i)};
                                }
                                if (!latest || latest->ts_off < cl.ts_off ||
                                    click_keys.less(cl.click_id, latest->click_id)) {
                                    latest = &cl;
                                    if (camp_dense) latest_row = camp_totals.credit_row(cl.campaign);
                                }
                            }
                            while (first < sessions.size()) credit(sessions[first++]);
                        }

                        e_lo = e_hi;
                        c_lo = c_hi;
                    }
                }
            }
            pq_out.finish();
            if (pq_out.wrote_any) any_users.store(true, std::memory_order_relaxed);
        } catch (...) {
            next_shard.store(SHARDS, std::memory_order_relaxed);
            std::lock_guard lk(err_mu);
            if (!err) err = std::current_exception();
        }
    };

    global_pool().run(threads, worker_fn);
    if (err) std::rethrow_exception(err);

    if (!any_users.load(std::memory_order_relaxed)) {
        pq::ParquetFileWriter empty_w((out_dir / "user_daily/part-0.parquet").string(), user_daily_schema());
        empty_w.close();
    }
    mark("sessions, attribution and user output");

    write_campaign_daily(out_dir / "campaign_daily/part-0.parquet", worker_camps, camp_layout, catalog);
    mark("campaign output");
    return true;
}

// ---------------- WIDE PIPELINE (100% general fallback) ----------------
void run_wide(const fs::path& data_dir, const fs::path& out_dir, unsigned threads,
              const Window& win, const FastTime& ft, const Catalog& catalog,
              RawKeys<'u'>& user_keys, const People& people,
              const pq::ByteBuf& cat_dict, CampaignLayout camp_layout) {
    constexpr size_t EV_BUCKETS = 256;
    const int64_t click_start = win.start - REACH_MS;
    RawKeys<'e'> ev_keys;
    std::vector<WorkerArena> ev_arenas(threads), dedup_arenas(threads);

    std::vector<std::array<SlabList<WideEv>, SHARDS>> ev_slabs(threads);
    std::vector<std::array<SlabList<WideDedup>, EV_BUCKETS>> dedup_slabs(threads);
    {
        PreadInput ev_in(data_dir / "events", 2u << 20);
        ev_in.scan<7>(threads, [&](unsigned w, const std::array<SV, 7>& f) {
            if (f[0].empty()) return;
            const int64_t ingest_ms = ft.parse_ingest(f[3]);
            if (ingest_ms == INVALID_TS) return;
            const uint64_t ev_key = ev_keys.encode(f[0]);
            uint32_t loc = INVALID_LOC;
            const int etype = parse_event_type(f[4]);
            if (etype != 0 && !f[1].empty() && !f[5].empty()) {
                const int64_t ts = ft.parse_ts(f[2]);
                if (ts >= win.start && ts < win.end && ingest_ms != INVALID_TS && (ingest_ms - ts) <= DAY_MS) {
                    uint32_t pr = 0, cat = 0;
                    bool ok = true;
                    if (etype == 2) {
                        int32_t p = parse_price(f[6]);
                        if (p <= 0) ok = false;
                        else { pr = uint32_t(p); cat = catalog.category(f[5]); }
                    }
                    if (ok) {
                        uint64_t person = people.resolve(user_keys.encode(f[1]));
                        uint32_t shard = uint32_t(fast_hash(person)) & uint32_t(SHARDS - 1);
                        loc = ev_slabs[w][shard].push_with_loc(ev_arenas[w], uint32_t(w) << 28,
                                                               {person, ts, pr, cat});
                    }
                }
            }
            uint32_t eb = uint32_t(fast_hash(ev_key)) & uint32_t(EV_BUCKETS - 1);
            dedup_slabs[w][eb].push(dedup_arenas[w], {ev_key, ingest_ms, loc, 0});
        }, [&](unsigned w) {
            for (auto& sl : ev_slabs[w]) sl.finalize();
            for (auto& sl : dedup_slabs[w]) sl.finalize();
        });
    }

    parallel_for(EV_BUCKETS, threads, [&](unsigned, size_t b) {
        size_t total = 0;
        for (unsigned w = 0; w < threads; ++w) total += dedup_slabs[w][b].total_count();
        if (total <= 1) return;
        size_t cap = 16;
        while (cap * 6 < total * 10) cap *= 2;
        const size_t mask = cap - 1;
        std::vector<uint64_t> ht_k(cap, 0);
        std::vector<int64_t> ht_ing(cap);
        std::vector<uint32_t> ht_loc(cap);
        auto mark_dead = [&](uint32_t loc) {
            reinterpret_cast<WideEv*>(ev_arenas[loc >> 28].slab_ptr((loc >> 10) & 0x3FFFFu))[loc & 1023u].ts = DEAD_TS_I64;
        };
        for (unsigned w = 0; w < threads; ++w) {
            for (const auto& slab : dedup_slabs[w][b].slabs()) {
                for (uint32_t i = 0; i < slab.count; ++i) {
                    const auto& r = slab.ptr[i];
                    size_t slot = size_t(fast_hash(r.ev_key) >> 16) & mask;
                    for (;;) {
                        if (ht_k[slot] == 0) {
                            ht_k[slot] = r.ev_key;
                            ht_ing[slot] = r.ingest;
                            ht_loc[slot] = r.loc;
                            break;
                        }
                        if (ht_k[slot] == r.ev_key) {
                            if (r.ingest < ht_ing[slot]) {
                                if (ht_loc[slot] != INVALID_LOC) mark_dead(ht_loc[slot]);
                                ht_ing[slot] = r.ingest;
                                ht_loc[slot] = r.loc;
                            } else {
                                if (r.loc != INVALID_LOC) mark_dead(r.loc);
                            }
                            break;
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            }
        }
    });

    for (unsigned w = 0; w < threads; ++w) dedup_arenas[w].reset();
    ClickKeys click_keys;
    std::vector<std::array<SlabList<WideClick>, SHARDS>> click_slabs(threads);
    {
        PreadInput cl_in(data_dir / "clicks", 2u << 20);
        cl_in.scan<4>(threads, [&](unsigned w, const std::array<SV, 4>& f) {
            if (f[0].empty() || f[1].empty()) return;
            const uint32_t camp = catalog.campaign(f[2]);
            if (camp == 0) return;
            const int64_t ts = ft.parse_ts(f[3]);
            if (ts < click_start || ts >= win.end) return;
            const uint64_t person = people.resolve(user_keys.encode(f[1]));
            const uint32_t shard = uint32_t(fast_hash(person)) & uint32_t(SHARDS - 1);
            click_slabs[w][shard].push(dedup_arenas[w], {person, click_keys.encode(f[0]), ts, camp, 0});
        }, [&](unsigned w) {
            for (auto& sl : click_slabs[w]) sl.finalize();
        });
    }

    fs::create_directories(out_dir / "user_daily");
    fs::create_directories(out_dir / "campaign_daily");
    std::vector<WorkerCampaigns> worker_camps;
    for (unsigned w = 0; w < threads; ++w) worker_camps.emplace_back(camp_layout);

    std::atomic<size_t> next_shard{0};
    std::atomic<bool> any_users{false};
    auto worker_fn = [&](unsigned wid) {
        UserParquetStream pq_out((out_dir / "user_daily" / ("part-" + std::to_string(wid) + ".parquet")).string(),
                                 cat_dict, int32_t(catalog.categories.size()),
                                 camp_layout.first_day, camp_layout.days);
        auto& camp_totals = worker_camps[wid];
        std::vector<WideEv> evs;
        std::vector<WideClick> cls;
        std::vector<SessionRec> sessions;
        std::vector<TempUserDay> udays;
        std::vector<CoverageRec> covered(catalog.campaigns.size(), {0, 0});
        DailyCats daily_cats(catalog.categories.size());
        uint32_t user_gen = 0;
        char ustr_buf[9];

        for (;;) {
            size_t shard = next_shard.fetch_add(1, std::memory_order_relaxed);
            if (shard >= SHARDS) break;
            evs.clear();
            for (unsigned w = 0; w < threads; ++w)
                for (const auto& sl : ev_slabs[w][shard].slabs())
                    for (uint32_t i = 0; i < sl.count; ++i)
                        if (sl.ptr[i].ts != DEAD_TS_I64) evs.push_back(sl.ptr[i]);
            if (evs.empty()) continue;
            cls.clear();
            for (unsigned w = 0; w < threads; ++w)
                for (const auto& sl : click_slabs[w][shard].slabs())
                    for (uint32_t i = 0; i < sl.count; ++i)
                        cls.push_back(sl.ptr[i]);

            std::sort(evs.begin(), evs.end(), [](const WideEv& a, const WideEv& b) {
                return a.person != b.person ? a.person < b.person : a.ts < b.ts;
            });
            std::sort(cls.begin(), cls.end(), [](const WideClick& a, const WideClick& b) {
                return a.person != b.person ? a.person < b.person : a.ts < b.ts;
            });

            size_t ei = 0, ci = 0;
            while (ei < evs.size()) {
                const uint64_t person = evs[ei].person;
                sessions.clear();
                udays.clear();
                uint32_t first_sess_of_day = 0;
                int32_t cur_day = int32_t(time_detail::day_number(evs[ei].ts));
                int32_t day_sess = 0, day_evs = 0;
                int64_t day_rev = 0;
                auto close_day = [&]() {
                    udays.push_back({cur_day, day_sess, day_evs, day_rev, daily_cats.finish_day()});
                    const uint32_t se = uint32_t(sessions.size());
                    while (first_sess_of_day < se) sessions[first_sess_of_day++].day_end = se;
                };
                do {
                    const size_t s_start_idx = ei;
                    const int64_t s_start = evs[ei].ts;
                    const int32_t d = int32_t(time_detail::day_number(s_start));
                    if (d != cur_day) {
                        close_day();
                        cur_day = d;
                        day_sess = 0; day_evs = 0; day_rev = 0;
                    }
                    int64_t s_rev = 0;
                    int64_t prev_ts = s_start;
                    for (;;) {
                        if (evs[ei].price) {
                            s_rev += evs[ei].price;
                            daily_cats.add(evs[ei].category, evs[ei].price);
                        }
                        ++ei;
                        if (ei >= evs.size() || evs[ei].person != person) break;
                        if (evs[ei].ts - prev_ts > GAP_MS) break;
                        prev_ts = evs[ei].ts;
                    }
                    ++day_sess;
                    day_evs += int32_t(ei - s_start_idx);
                    day_rev += s_rev;
                    sessions.push_back({s_start, s_rev, d, 0});
                } while (ei < evs.size() && evs[ei].person == person);
                close_day();

                SV ustr = user_keys.decode(person, ustr_buf);
                pq_out.begin_user(ustr, uint32_t(udays.size()));
                for (const auto& ud : udays)
                    pq_out.add_day(ud.day, ud.sessions, ud.events, ud.revenue, ud.top_cat);

                while (ci < cls.size() && cls[ci].person < person) ++ci;
                ++user_gen;
                if (user_gen == 0) {
                    std::fill(covered.begin(), covered.end(), CoverageRec{0, 0});
                    user_gen = 1;
                }
                size_t first = 0, end_i = 0;
                const WideClick* latest = nullptr;
                auto credit = [&](const SessionRec& s) {
                    const uint32_t camp = (latest && (s.start - latest->ts) <= REACH_MS) ? latest->campaign : 0u;
                    camp_totals.add_credit(camp, s.day, s.revenue);
                };
                while (ci < cls.size() && cls[ci].person == person) {
                    const WideClick& cl = cls[ci++];
                    while (first < sessions.size() && sessions[first].start < cl.ts) credit(sessions[first++]);
                    if (end_i < first) end_i = first;
                    while (end_i < sessions.size() && (sessions[end_i].start - cl.ts) <= REACH_MS) ++end_i;
                    if (first < end_i) {
                        CoverageRec& prior = covered[cl.campaign];
                        size_t pos = (prior.gen == user_gen) ? std::max(first, size_t(prior.end)) : first;
                        while (pos < end_i) {
                            const SessionRec& s = sessions[pos];
                            size_t nxt = std::min(end_i, size_t(s.day_end));
                            camp_totals.add_assists(cl.campaign, s.day, uint32_t(nxt - pos));
                            pos = nxt;
                        }
                        prior = {user_gen, uint32_t(end_i)};
                    }
                    if (!latest || latest->ts < cl.ts || click_keys.less(cl.click_id, latest->click_id))
                        latest = &cl;
                }
                while (first < sessions.size()) credit(sessions[first++]);
            }
        }
        pq_out.finish();
        if (pq_out.wrote_any) any_users.store(true, std::memory_order_relaxed);
    };
    std::vector<std::thread> pool;
    for (unsigned w = 1; w < threads; ++w) pool.emplace_back(worker_fn, w);
    worker_fn(0);
    for (auto& t : pool) t.join();
    if (!any_users.load(std::memory_order_relaxed)) {
        pq::ParquetFileWriter empty_w((out_dir / "user_daily/part-0.parquet").string(), user_daily_schema());
        empty_w.close();
    }
    write_campaign_daily(out_dir / "campaign_daily/part-0.parquet", worker_camps, camp_layout, catalog);
}

void run_pipeline(const fs::path& data_dir, const fs::path& out_dir, unsigned threads) {
    const bool profile = std::getenv("PROFILE") != nullptr;
    auto t0 = std::chrono::steady_clock::now();

    Window win(data_dir);
    FastTime ft;
    ft.init(win.start, win.end);
    global_pool().ensure(threads);

    std::unique_ptr<Catalog> catalog_ptr;
    std::exception_ptr cat_err;
    std::thread cat_thr;
    if (threads > 1) {
        cat_thr = std::thread([&]() {
            try {
                catalog_ptr = std::make_unique<Catalog>(data_dir);
            } catch (...) {
                cat_err = std::current_exception();
            }
        });
    } else {
        catalog_ptr = std::make_unique<Catalog>(data_dir);
    }

    RawKeys<'u'> user_keys;
    People people(data_dir, user_keys, threads);
    if (cat_thr.joinable()) cat_thr.join();
    if (cat_err) std::rethrow_exception(cat_err);
    const Catalog& catalog = *catalog_ptr;

    if (profile) {
        auto now = std::chrono::steady_clock::now();
        rusage ru{};
        ::getrusage(RUSAGE_SELF, &ru);
        std::cerr << "catalogs and links wall=" << std::chrono::duration<double>(now - t0).count()
                  << "s user=" << (ru.ru_utime.tv_sec + ru.ru_utime.tv_usec * 1e-6)
                  << "s sys=" << (ru.ru_stime.tv_sec + ru.ru_stime.tv_usec * 1e-6) << "s\n";
    }

    pq::ByteBuf cat_dict;
    for (const auto& c : catalog.categories) {
        cat_dict.push_u32_le(uint32_t(c.size()));
        cat_dict.append(c.data(), c.size());
    }

    const int32_t first_day = int32_t(time_detail::day_number(win.start));
    const int32_t last_day = int32_t(time_detail::day_number(win.end - 1));
    const uint64_t num_days = uint64_t(int64_t(last_day) - int64_t(first_day) + 1);
    const uint64_t slots = num_days * catalog.campaigns.size();
    CampaignLayout camp_layout{first_day, uint32_t(num_days), uint32_t(catalog.campaigns.size()),
                               slots > 0 && slots <= 262144};

    const bool can_compact =
        (uint64_t(win.end - win.start) <= 0xFFFFFFFEULL) &&
        (uint64_t(win.end - (win.start - REACH_MS)) <= 0xFFFFFFFEULL) &&
        (catalog.categories.size() <= 256) &&
        (catalog.campaigns.size() <= 65535) &&
        (threads <= 15);

    if (can_compact) {
        PreadInput ev_in(data_dir / "events", 2u << 20);
        const size_t ev_buckets = (ev_in.total_bytes > (2500ULL << 20)) ? 1024u : 512u;
        if (run_compact(ev_in, ev_buckets, data_dir, out_dir, threads, win, ft, catalog,
                        user_keys, people, cat_dict, camp_layout, profile)) {
            return;
        }
    }
    run_wide(data_dir, out_dir, threads, win, ft, catalog, user_keys, people, cat_dict, camp_layout);
}

} // namespace blitz

int main(int argc, char** argv) {
    try {
        auto get_path = [&](int i, const char* env, const char* fallback) -> std::string {
            if (argc > i) return argv[i];
            const char* v = std::getenv(env);
            return v ? v : fallback;
        };
        const char* cpu_env = std::getenv("NCPU");
        unsigned threads = cpu_env ? static_cast<unsigned>(std::clamp(std::atoi(cpu_env), 1, 15)) : 8u;
        blitz::run_pipeline(get_path(1, "DATA_DIR", "/data"),
                            get_path(2, "OUTPUT_DIR", "/output"),
                            threads);
        if (std::getenv("PROFILE")) std::cerr.flush();
        if (!std::getenv("NO_FAST_EXIT")) ::_exit(0);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Pipeline failed: " << e.what() << '\n';
        return 1;
    }
}
