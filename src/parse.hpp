#pragma once

#include <array>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <immintrin.h>
#include <limits>
#include <string_view>

namespace blitz {

using SV = std::string_view;

inline constexpr int64_t INVALID_TS = std::numeric_limits<int64_t>::min();
inline constexpr int64_t DAY_MS = 86'400'000;

namespace time_detail {

inline bool is_leap(int y) {
    return (y % 4 == 0) && (y % 100 != 0 || y % 400 == 0);
}

inline int days_in_month(int y, int m) {
    static constexpr int L[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return L[m - 1] + (m == 2 && is_leap(y));
}

// Howard Hinnant's days_from_civil (exact proleptic Gregorian for all years).
inline int64_t days_from_civil(int y, int m, int d) {
    y -= (m <= 2);
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + static_cast<unsigned>(d) - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return int64_t(era) * 146097 + int64_t(doe) - 719468;
}

inline void civil_from_days(int64_t z, int& y, int& m, int& d) {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    y = int(yoe) + int(era) * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    d = int(doy - (153 * mp + 2) / 5 + 1);
    m = int(mp + (mp < 10 ? 3 : -9));
    y += (m <= 2);
}

inline int64_t day_number(int64_t ms) {
    return ms / DAY_MS - (ms % DAY_MS < 0);
}

inline bool digits2(const char* p) {
    return unsigned(p[0] - '0') < 10 && unsigned(p[1] - '0') < 10;
}

inline unsigned pair2(const char* p) {
    return unsigned(p[0] - '0') * 10u + unsigned(p[1] - '0');
}

// General exact civil_ms for arbitrary valid 19-byte prefix YYYY-MM-DDTHH:MM:SS.
inline int64_t civil_ms_general(const char* p) {
    if (!digits2(p) || !digits2(p + 2) || p[4] != '-' || !digits2(p + 5) || p[7] != '-' ||
        !digits2(p + 8) || p[10] != 'T' || !digits2(p + 11) || p[13] != ':' ||
        !digits2(p + 14) || p[16] != ':' || !digits2(p + 17))
        return INVALID_TS;
    int y = int(pair2(p) * 100 + pair2(p + 2));
    int m = int(pair2(p + 5));
    int d = int(pair2(p + 8));
    unsigned h = pair2(p + 11), mi = pair2(p + 14), s = pair2(p + 17);
    if (m < 1 || m > 12 || d < 1 || d > days_in_month(y, m) || h >= 24 || mi >= 60 || s >= 60)
        return INVALID_TS;
    return days_from_civil(y, m, d) * DAY_MS + int64_t(h * 3600 + mi * 60 + s) * 1000LL;
}

inline int64_t parse_iso_general(SV s) {
    const size_t n = s.size();
    const char* p = s.data();
    if (n == 20 && p[19] == 'Z') return civil_ms_general(p);
    if (n == 25 && (p[19] == '+' || p[19] == '-') && p[22] == ':' &&
        digits2(p + 20) && digits2(p + 23)) {
        unsigned oh = pair2(p + 20), om = pair2(p + 23);
        if (oh >= 24 || om >= 60) return INVALID_TS;
        int64_t local = civil_ms_general(p);
        if (local == INVALID_TS) return INVALID_TS;
        int64_t off = int64_t(oh * 60 + om) * 60000LL;
        return p[19] == '+' ? (local - off) : (local + off);
    }
    return INVALID_TS;
}

inline bool parse_8digits_swar(const char* p, uint32_t& out) {
    uint64_t val;
    std::memcpy(&val, p, 8);
    val -= 0x3030303030303030ULL;
    if (__builtin_expect(((val | (val + 0x0606060606060606ULL)) & 0xF0F0F0F0F0F0F0F0ULL) != 0, 0))
        return false;
    val = (val * 10 + (val >> 8)) & 0x00FF00FF00FF00FFULL;
    val = (val * 100 + (val >> 16)) & 0x0000FFFF0000FFFFULL;
    val = (val * 10000 + (val >> 32)) & 0x00000000FFFFFFFFULL;
    out = static_cast<uint32_t>(val);
    return true;
}

inline int64_t parse_f3_13(const char* p) {
    // Parse first 5 digits + last 8 digits in GPR SWAR (no XMM-GPR transfer).
    uint64_t raw5;
    std::memcpy(&raw5, p, 8);
    uint64_t hi = ((raw5 & 0x000000FFFFFFFFFFULL) << 24) - 0x3030303030000000ULL;
    uint64_t lo;
    std::memcpy(&lo, p + 5, 8);
    lo -= 0x3030303030303030ULL;
    uint64_t bad = ((hi | (hi + 0x0606060606000000ULL)) | (lo | (lo + 0x0606060606060606ULL))) &
                   0xF0F0F0F0F0F0F0F0ULL;
    if (__builtin_expect(bad != 0, 0)) return INVALID_TS;

    hi = (hi * 10 + (hi >> 8)) & 0x00FF00FF00FF00FFULL;
    hi = (hi * 100 + (hi >> 16)) & 0x0000FFFF0000FFFFULL;
    hi = (hi * 10000 + (hi >> 32)) & 0x00000000FFFFFFFFULL;

    lo = (lo * 10 + (lo >> 8)) & 0x00FF00FF00FF00FFULL;
    lo = (lo * 100 + (lo >> 16)) & 0x0000FFFF0000FFFFULL;
    lo = (lo * 10000 + (lo >> 32)) & 0x00000000FFFFFFFFULL;

    return int64_t(hi) * 100'000'000LL + int64_t(lo);
}

inline int64_t parse_f3_general(SV s) {
    const size_t n = s.size();
    const char* p = s.data();
    if (n == 0 || unsigned(p[0] - '0') >= 10) return INVALID_TS;
    if (n <= 18) {
        int64_t v = 0;
        for (size_t i = 0; i < n; ++i) {
            unsigned d = unsigned(p[i] - '0');
            if (d >= 10) return INVALID_TS;
            v = v * 10 + int64_t(d);
        }
        return v;
    }
    // Verify all ASCII digits first (handles arbitrary leading zeros & >64-bit numbers).
    for (size_t i = 0; i < n; ++i) {
        if (unsigned(p[i] - '0') >= 10) return INVALID_TS;
    }
    // Strip leading zeros before parsing 64-bit integer.
    size_t k = 0;
    while (k < n && p[k] == '0') ++k;
    if (k == n) return 0;
    int64_t v = 0;
    auto r = std::from_chars(p + k, p + n, v);
    return (r.ec == std::errc{} && r.ptr == p + n) ? v : std::numeric_limits<int64_t>::max();
}

} // namespace time_detail

// Fast window-aware timestamp parser. Precomputes valid YYYY-MM- prefixes
// covering [window_start - 9 days, window_end + 8 days] so F1/F2 parsing
// hits zero calendar tables in memory while remaining 100% exact on any window.
struct FastTime {
    struct MonthEntry {
        uint64_t ym_prefix;    // 8 bytes "YYYY-MM-"
        int64_t day0_sec;      // epoch sec of YYYY-MM-01 minus 1 day (add day*86400)
        int64_t day0_ms;       // day0_sec * 1000
        uint32_t max_day;
    };

    int64_t win_start = 0;
    int64_t win_end = 0;
    int64_t click_start = 0;
    bool use_fast_months = false;
    uint32_t num_months = 0;
    uint32_t primary_month = 0;
    std::array<MonthEntry, 6> months{};

    void init(int64_t start, int64_t end) {
        win_start = start;
        win_end = end;
        click_start = start - 7 * DAY_MS;

        int64_t d_lo = time_detail::day_number(start - 9 * DAY_MS);
        int64_t d_hi = time_detail::day_number(end + 8 * DAY_MS);
        int y1, m1, day1, y2, m2, day2;
        time_detail::civil_from_days(d_lo, y1, m1, day1);
        time_detail::civil_from_days(d_hi, y2, m2, day2);

        int y_mid, m_mid, d_mid;
        time_detail::civil_from_days(time_detail::day_number(start + (end - start) / 2), y_mid, m_mid, d_mid);

        num_months = 0;
        primary_month = 0;
        use_fast_months = true;
        int y = y1, m = m1;
        while (y < y2 || (y == y2 && m <= m2)) {
            if (num_months >= months.size() || y < 0 || y > 9999) {
                use_fast_months = false;
                break;
            }
            char buf[8];
            buf[0] = char('0' + (y / 1000) % 10);
            buf[1] = char('0' + (y / 100) % 10);
            buf[2] = char('0' + (y / 10) % 10);
            buf[3] = char('0' + y % 10);
            buf[4] = '-';
            buf[5] = char('0' + m / 10);
            buf[6] = char('0' + m % 10);
            buf[7] = '-';
            uint64_t pfx;
            std::memcpy(&pfx, buf, 8);
            int64_t first_day_sec = time_detail::days_from_civil(y, m, 1) * 86400LL;
            if (y == y_mid && m == m_mid) primary_month = num_months;
            months[num_months++] = {pfx, first_day_sec - 86400LL, (first_day_sec - 86400LL) * 1000LL,
                                    uint32_t(time_detail::days_in_month(y, m))};
            if (++m == 13) {
                m = 1;
                ++y;
            }
        }
        // Put the most frequent month at index 0 so 1st compare hits on 50-100% of rows!
        if (use_fast_months && primary_month != 0) {
            std::swap(months[0], months[primary_month]);
            primary_month = 0;
        }
    }

    inline int find_month(uint64_t pfx) const {
        if (__builtin_expect(months[0].ym_prefix == pfx, 1)) return 0;
        for (uint32_t i = 1; i < num_months; ++i) {
            if (months[i].ym_prefix == pfx) return int(i);
        }
        return -1;
    }

    // Parse suffix DDTHH:MM:SS (starting at p+8) given matched month entry.
    inline int64_t parse_month_local(const char* p, const MonthEntry& me) const {
        uint64_t lo;
        uint32_t hi;
        std::memcpy(&lo, p + 8, 8);  // DDTHH:MM
        std::memcpy(&hi, p + 16, 4); // :SS?
        if (__builtin_expect(((lo & 0x0000FF0000FF0000ULL) != 0x00003A0000540000ULL) ||
                             ((hi & 0x000000FFU) != 0x3AU), 0))
            return INVALID_TS;
        uint64_t d6 = ((lo & 0xFFFF00FFFF00FFFFULL) | 0x0000300000300000ULL) - 0x3030303030303030ULL;
        uint32_t d2 = ((hi >> 8) & 0xFFFFU) - 0x3030U;
        uint64_t chk = (d6 | (d6 + 0x0606060606060606ULL)) | uint64_t(d2 | (d2 + 0x0606U));
        if (__builtin_expect((chk & 0xF0F0F0F0F0F0F0F0ULL) != 0, 0)) return INVALID_TS;

        uint32_t day = uint32_t(d6 & 0xFF) * 10u + uint32_t((d6 >> 8) & 0xFF);
        uint32_t h   = uint32_t((d6 >> 24) & 0xFF) * 10u + uint32_t((d6 >> 32) & 0xFF);
        uint32_t mi  = uint32_t((d6 >> 48) & 0xFF) * 10u + uint32_t((d6 >> 56) & 0xFF);
        uint32_t sec = uint32_t(d2 & 0xFF) * 10u + uint32_t((d2 >> 8) & 0xFF);
        if (__builtin_expect((day - 1u) >= me.max_day || h >= 24u || mi >= 60u || sec >= 60u, 0))
            return INVALID_TS;
        return me.day0_ms + int64_t(day) * DAY_MS + int64_t(h * 3600u + mi * 60u + sec) * 1000LL;
    }

    // Parse THH:MM:SSZ given an already-validated YYYY-MM-DD day_ms!
    inline int64_t parse_hms_z(const char* p, int64_t day_ms) const {
        uint64_t lo;
        uint32_t hi;
        std::memcpy(&lo, p + 8, 8);  // DDTHH:MM
        std::memcpy(&hi, p + 16, 4); // :SSZ
        if (__builtin_expect(((lo & 0x0000FF0000FF0000ULL) != 0x00003A0000540000ULL) ||
                             ((hi & 0xFF0000FFU) != 0x5A00003AU), 0))
            return INVALID_TS;
        uint64_t d6 = ((lo & 0xFFFF00FFFF000000ULL) | 0x0000300000303030ULL) - 0x3030303030303030ULL;
        uint32_t d2 = ((hi >> 8) & 0xFFFFU) - 0x3030U;
        uint64_t chk = (d6 | (d6 + 0x0606060606060606ULL)) | uint64_t(d2 | (d2 + 0x0606U));
        if (__builtin_expect((chk & 0xF0F0F0F0F0F0F0F0ULL) != 0, 0)) return INVALID_TS;
        uint32_t h   = uint32_t((d6 >> 24) & 0xFF) * 10u + uint32_t((d6 >> 32) & 0xFF);
        uint32_t mi  = uint32_t((d6 >> 48) & 0xFF) * 10u + uint32_t((d6 >> 56) & 0xFF);
        uint32_t sec = uint32_t(d2 & 0xFF) * 10u + uint32_t((d2 >> 8) & 0xFF);
        if (__builtin_expect(h >= 24u || mi >= 60u || sec >= 60u, 0)) return INVALID_TS;
        return day_ms + int64_t(h * 3600u + mi * 60u + sec) * 1000LL;
    }

    // Parse `ingest_ts` returning BOTH epoch seconds and day midnight ms (for same-date ts shortcut!).
    inline int64_t parse_ingest_sec(SV s, int64_t& out_day_ms) const {
        if (__builtin_expect(s.size() != 20, 0)) return INVALID_TS;
        const char* p = s.data();
        if (__builtin_expect(use_fast_months, 1)) {
            uint64_t pfx;
            std::memcpy(&pfx, p, 8);
            int idx = find_month(pfx);
            if (__builtin_expect(idx >= 0, 1)) {
                const auto& me = months[idx];
                uint64_t lo;
                uint32_t hi;
                std::memcpy(&lo, p + 8, 8);
                std::memcpy(&hi, p + 16, 4);
                if (__builtin_expect(((lo & 0x0000FF0000FF0000ULL) != 0x00003A0000540000ULL) ||
                                     ((hi & 0xFF0000FFU) != 0x5A00003AU), 0))
                    return INVALID_TS;
                uint64_t d6 = ((lo & 0xFFFF00FFFF00FFFFULL) | 0x0000300000300000ULL) - 0x3030303030303030ULL;
                uint32_t d2 = ((hi >> 8) & 0xFFFFU) - 0x3030U;
                uint64_t chk = (d6 | (d6 + 0x0606060606060606ULL)) | uint64_t(d2 | (d2 + 0x0606U));
                if (__builtin_expect((chk & 0xF0F0F0F0F0F0F0F0ULL) != 0, 0)) return INVALID_TS;
                uint32_t day = uint32_t(d6 & 0xFF) * 10u + uint32_t((d6 >> 8) & 0xFF);
                uint32_t h   = uint32_t((d6 >> 24) & 0xFF) * 10u + uint32_t((d6 >> 32) & 0xFF);
                uint32_t mi  = uint32_t((d6 >> 48) & 0xFF) * 10u + uint32_t((d6 >> 56) & 0xFF);
                uint32_t sec = uint32_t(d2 & 0xFF) * 10u + uint32_t((d2 >> 8) & 0xFF);
                if (__builtin_expect((day - 1u) >= me.max_day || h >= 24u || mi >= 60u || sec >= 60u, 0))
                    return INVALID_TS;
                int64_t dsec = me.day0_sec + int64_t(day) * 86400LL;
                out_day_ms = dsec * 1000LL;
                return dsec + int64_t(h * 3600u + mi * 60u + sec);
            }
        }
        if (__builtin_expect(p[19] != 'Z', 0)) return INVALID_TS;
        int64_t ms = time_detail::civil_ms_general(p);
        if (ms == INVALID_TS) return INVALID_TS;
        out_day_ms = time_detail::day_number(ms) * DAY_MS;
        return ms / 1000LL;
    }

    // Fast event `ts` parse reusing `ingest_ts`'s validated YYYY-MM-DD when identical (~58% of rows!).
    inline int64_t parse_ts_with_ingest(SV ts_sv, SV ing_sv, int64_t ing_day_ms) const {
        const size_t n = ts_sv.size();
        const char* p = ts_sv.data();
        if (n == 20) {
            uint64_t pfx;
            std::memcpy(&pfx, p, 8);
            uint64_t ipfx;
            std::memcpy(&ipfx, ing_sv.data(), 8);
            uint16_t d2, id2;
            std::memcpy(&d2, p + 8, 2);
            std::memcpy(&id2, ing_sv.data() + 8, 2);
            if (__builtin_expect(pfx == ipfx && d2 == id2, 1)) {
                return parse_hms_z(p, ing_day_ms);
            }
            if (__builtin_expect(p[19] != 'Z', 0)) return INVALID_TS;
            if (__builtin_expect(use_fast_months, 1)) {
                int idx = find_month(pfx);
                if (__builtin_expect(idx < 0, 0)) return INVALID_TS;
                return parse_month_local(p, months[idx]);
            }
            return time_detail::civil_ms_general(p);
        }
        if (n == 13) return time_detail::parse_f3_13(p);
        return parse_ts(ts_sv);
    }

    // Parse `ts` for events or clicks. Returns INVALID_TS if malformed OR
    // guaranteed outside [click_start, win_end) (for fast-month mode).
    inline int64_t parse_ts(SV s) const {
        const size_t n = s.size();
        const char* p = s.data();
        if (__builtin_expect(n == 20, 1)) {
            if (__builtin_expect(p[19] != 'Z', 0)) return INVALID_TS;
            if (__builtin_expect(use_fast_months, 1)) {
                uint64_t pfx;
                std::memcpy(&pfx, p, 8);
                int idx = find_month(pfx);
                if (__builtin_expect(idx < 0, 0)) return INVALID_TS; // guaranteed outside window
                return parse_month_local(p, months[idx]);
            }
            return time_detail::civil_ms_general(p);
        }
        if (__builtin_expect(n == 13, 1)) {
            return time_detail::parse_f3_13(p);
        }
        if (n == 25) {
            if ((p[19] != '+' && p[19] != '-') || p[22] != ':' ||
                !time_detail::digits2(p + 20) || !time_detail::digits2(p + 23))
                return INVALID_TS;
            unsigned oh = time_detail::pair2(p + 20), om = time_detail::pair2(p + 23);
            if (oh >= 24 || om >= 60) return INVALID_TS;
            int64_t local;
            if (__builtin_expect(use_fast_months, 1)) {
                uint64_t pfx;
                std::memcpy(&pfx, p, 8);
                int idx = find_month(pfx);
                if (idx < 0) return INVALID_TS; // guaranteed outside window even with +-23:59
                local = parse_month_local(p, months[idx]);
            } else {
                local = time_detail::civil_ms_general(p);
            }
            if (local == INVALID_TS) return INVALID_TS;
            int64_t off = int64_t(oh * 60u + om) * 60000LL;
            return p[19] == '+' ? (local - off) : (local + off);
        }
        return time_detail::parse_f3_general(s);
    }

    // Parse `ingest_ts` (rules.md: always valid F1, can be outside window for very late rows).
    inline int64_t parse_ingest(SV s) const {
        if (__builtin_expect(s.size() != 20, 0)) return INVALID_TS;
        const char* p = s.data();
        if (__builtin_expect(use_fast_months, 1)) {
            uint64_t pfx;
            std::memcpy(&pfx, p, 8);
            int idx = find_month(pfx);
            if (__builtin_expect(idx >= 0, 1)) {
                // Fast path: ingest_ts is guaranteed valid F1 by rules.md.
                const auto& me = months[idx];
                uint32_t day = time_detail::pair2(p + 8);
                uint32_t h   = time_detail::pair2(p + 11);
                uint32_t mi  = time_detail::pair2(p + 14);
                uint32_t sec = time_detail::pair2(p + 17);
                return me.day0_ms + int64_t(day) * DAY_MS + int64_t(h * 3600u + mi * 60u + sec) * 1000LL;
            }
        }
        return time_detail::civil_ms_general(p);
    }
};

// Parse purchase price_cents: ASCII digits only, value in [1, 10'000'000], leading zeros allowed.
inline int32_t parse_price(SV s) {
    const size_t n = s.size();
    if (n == 0) return 0;
    const char* p = s.data();
    int32_t v = 0;
    for (size_t i = 0; i < n; ++i) {
        unsigned d = unsigned(p[i] - '0');
        if (__builtin_expect(d >= 10u, 0)) return 0;
        v = v * 10 + int32_t(d);
        if (__builtin_expect(v > 10'000'000, 0)) return 0;
    }
    return v;
}

// Scalar exact CSV field splitter (for rare lines > 128 bytes or small catalogs).
template <size_t N>
inline bool split_fields_slow(SV line, std::array<SV, N>& out) {
    const size_t len = line.size();
    if (len == 0) return false;
    const char* p = line.data();
    size_t start = 0, count = 0;
    auto emit = [&](size_t end) -> bool {
        if (count == N) return false;
        size_t s = start, e = end;
        if (s < e && p[s] == '"') {
            if (e - s < 2 || p[e - 1] != '"') return false;
            ++s;
            --e;
        }
        for (size_t k = s; k < e; ++k) {
            if (p[k] == '"') return false;
        }
        out[count++] = SV(p + s, e - s);
        start = end + 1;
        return true;
    };
    for (size_t i = 0; i < len; ++i) {
        if (p[i] == ',' && !emit(i)) return false;
    }
    return emit(len) && count == N;
}

// Branchless 128-bit mask-based CSV field extractor (ZERO PEXT, ZERO quote branches,
// ZERO comma-loop branches, ZERO len<=64 branches!).
// Precondition: 1 <= len <= 128; lo_cm/hi_cm/lo_qt/hi_qt already masked to `len` bits.
template <size_t N>
inline bool split_fields_128(const char* p, size_t len,
                             uint64_t lo_cm, uint64_t hi_cm,
                             uint64_t lo_qt, uint64_t hi_qt,
                             std::array<SV, N>& out) {
    if (__builtin_expect(__builtin_popcountll(lo_cm) + __builtin_popcountll(hi_cm) != int(N - 1), 0))
        return false;

    const __uint128_t cm128 = (static_cast<__uint128_t>(hi_cm) << 64) | lo_cm;
    const __uint128_t qt128 = (static_cast<__uint128_t>(hi_qt) << 64) | lo_qt;
    const __uint128_t starts = (cm128 << 1) | __uint128_t{1};
    const __uint128_t ends = (cm128 >> 1) | (__uint128_t{1} << (len - 1));
    const __uint128_t bad_qt = (qt128 & ~(starts | ends)) | (qt128 & starts & ends);
    if (__builtin_expect(bad_qt != 0, 0)) return false;

    auto next_bit = [](uint64_t& lo, uint64_t& hi) __attribute__((always_inline)) -> size_t {
        const bool use_lo = (lo != 0);
        const size_t b_lo = size_t(_tzcnt_u64(lo));
        const size_t b_hi = 64u + size_t(_tzcnt_u64(hi));
        const size_t pos = use_lo ? b_lo : b_hi;
        const uint64_t nlo = lo & (lo - 1);
        const uint64_t nhi = hi & (hi - 1);
        lo = use_lo ? nlo : lo;
        hi = use_lo ? hi : nhi;
        return pos;
    };

    size_t s = 0;
    size_t bad_pair = 0;
    auto emit = [&](size_t idx, size_t cpos) __attribute__((always_inline)) {
        const size_t flen = cpos - s;
        const size_t non_empty = (flen != 0);
        const size_t last = non_empty ? (cpos - 1) : s;
        const size_t q0 = non_empty & size_t(p[s] == '"');
        const size_t q1 = non_empty & size_t(p[last] == '"');
        bad_pair |= (q0 ^ q1);
        out[idx] = SV(p + s + q0, flen - 2 * q0);
        s = cpos + 1;
    };

    [&]<size_t... I>(std::index_sequence<I...>) __attribute__((always_inline)) {
        (emit(I, next_bit(lo_cm, hi_cm)), ...);
    }(std::make_index_sequence<N - 1>{});
    emit(N - 1, len);
    return __builtin_expect(bad_pair == 0, 1);
}

struct ChunkMasks {
    uint64_t nl, cm, qt;
};

inline void scan_blocks_avx2(const char* p, size_t nchunks, ChunkMasks* out) {
    const __m256i c_nl = _mm256_set1_epi8('\n');
    const __m256i c_cm = _mm256_set1_epi8(',');
    const __m256i c_qt = _mm256_set1_epi8('"');
    for (size_t i = 0; i < nchunks; ++i, p += 64) {
        const __m256i v0 = _mm256_load_si256(reinterpret_cast<const __m256i*>(p));
        const __m256i v1 = _mm256_load_si256(reinterpret_cast<const __m256i*>(p + 32));
        out[i] = {
            uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, c_nl))) |
                (uint64_t(uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, c_nl)))) << 32),
            uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, c_cm))) |
                (uint64_t(uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, c_cm)))) << 32),
            uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v0, c_qt))) |
                (uint64_t(uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v1, c_qt)))) << 32)
        };
    }
}

#if defined(__GNUC__) || defined(__clang__)
__attribute__((target("avx512bw,avx512vl"), noinline))
inline void scan_blocks_avx512(const char* p, size_t nchunks, ChunkMasks* out) {
    const __m512i c_nl = _mm512_set1_epi8('\n');
    const __m512i c_cm = _mm512_set1_epi8(',');
    const __m512i c_qt = _mm512_set1_epi8('"');
    for (size_t i = 0; i < nchunks; ++i, p += 64) {
        const __m512i v = _mm512_load_si512(reinterpret_cast<const void*>(p));
        out[i] = {
            uint64_t(_mm512_cmpeq_epi8_mask(v, c_nl)),
            uint64_t(_mm512_cmpeq_epi8_mask(v, c_cm)),
            uint64_t(_mm512_cmpeq_epi8_mask(v, c_qt))
        };
    }
}
#endif

inline bool has_avx512_bw_vl() {
#if defined(__GNUC__) || defined(__clang__)
    static const bool supported = [] {
        __builtin_cpu_init();
        return __builtin_cpu_supports("avx512bw") && __builtin_cpu_supports("avx512vl");
    }();
    return supported;
#else
    return false;
#endif
}

// Stream a 64-byte aligned buffer `[buf, buf + valid_len)` (which MUST have at least
// 64 bytes of readable zero padding past `valid_len`) in a single SIMD pass, invoking
// `on_row(std::array<SV, N>&)` for every structurally valid N-field CSV line.
template <size_t N, class OnRow>
inline size_t scan_csv_buffer(const char* buf, size_t valid_len, OnRow&& on_row) {
    size_t line_start = 0;
    std::array<SV, N> f;
    constexpr size_t BLK = 64; // 64 chunks = 4 KiB per mini-block
    ChunkMasks masks[BLK + 4]{}; // [0]=prev2, [1]=prev1, [2..BLK+1]=current, [BLK+2..BLK+3]=zero guard
    const bool use512 = has_avx512_bw_vl();

    const size_t total_chunks = (valid_len + 63) >> 6;
    for (size_t base_c = 0; base_c < total_chunks; base_c += BLK) {
        const size_t nchunks = std::min(BLK, total_chunks - base_c);
#if defined(__GNUC__) || defined(__clang__)
        if (use512) scan_blocks_avx512(buf + (base_c << 6), nchunks, masks + 2);
        else
#endif
            scan_blocks_avx2(buf + (base_c << 6), nchunks, masks + 2);

        // Mask the final chunk if valid_len is not a multiple of 64.
        if (__builtin_expect(base_c + nchunks == total_chunks && (valid_len & 63) != 0, 0)) {
            const uint64_t keep = (uint64_t{1} << (valid_len & 63)) - 1;
            auto& last = masks[2 + nchunks - 1];
            last.nl &= keep;
            last.cm &= keep;
            last.qt &= keep;
        }

        for (size_t k = 0; k < nchunks; ++k) {
            uint64_t nl = masks[k + 2].nl;
            if (__builtin_expect(nl == 0, 0)) continue;
            const size_t pos = (base_c + k) << 6;
            const size_t e_chunk = base_c + k;
            do {
                const size_t nl_bit = size_t(__builtin_ctzll(nl));
                nl &= nl - 1;
                const size_t line_end = pos + nl_bit;
                const size_t len = line_end - line_start;
                const size_t s_chunk = line_start >> 6;
                const size_t diff = e_chunk - s_chunk;
                bool ok;
                if (__builtin_expect(len > 0 && len <= 128 && diff <= 2, 1)) {
                    const size_t idx = (k + 2) - diff;
                    const unsigned s_off = unsigned(line_start & 63);
                    const ChunkMasks& m0 = masks[idx];
                    const ChunkMasks& m1 = masks[idx + 1];
                    const ChunkMasks& m2 = masks[idx + 2];
                    // Compiles to 4 branchless shrd instructions (s_off in 0..63 => zero UB!).
                    const uint64_t raw_lo_cm = static_cast<uint64_t>(((static_cast<__uint128_t>(m1.cm) << 64) | m0.cm) >> s_off);
                    const uint64_t raw_hi_cm = static_cast<uint64_t>(((static_cast<__uint128_t>(m2.cm) << 64) | m1.cm) >> s_off);
                    const uint64_t raw_lo_qt = static_cast<uint64_t>(((static_cast<__uint128_t>(m1.qt) << 64) | m0.qt) >> s_off);
                    const uint64_t raw_hi_qt = static_cast<uint64_t>(((static_cast<__uint128_t>(m2.qt) << 64) | m1.qt) >> s_off);
                    const __uint128_t lmask = (~__uint128_t{0}) >> (128u - unsigned(len));
                    const uint64_t lm_lo = static_cast<uint64_t>(lmask);
                    const uint64_t lm_hi = static_cast<uint64_t>(lmask >> 64);
                    ok = split_fields_128<N>(buf + line_start, len,
                                             raw_lo_cm & lm_lo, raw_hi_cm & lm_hi,
                                             raw_lo_qt & lm_lo, raw_hi_qt & lm_hi, f);
                } else {
                    ok = split_fields_slow<N>(SV(buf + line_start, len), f);
                }
                if (__builtin_expect(ok, 1)) on_row(f);
                line_start = line_end + 1;
            } while (nl);
        }

        if (nchunks >= 2) {
            masks[0] = masks[nchunks];
            masks[1] = masks[nchunks + 1];
        } else {
            masks[0] = masks[1];
            masks[1] = masks[2];
        }
    }
    return line_start;
}

} // namespace blitz
