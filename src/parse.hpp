#pragma once

#include <array>
#include <charconv>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>
#ifdef __SSE2__
#include <emmintrin.h>
#endif
#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace ss {
using SV = std::string_view;
namespace parse_detail {

struct Marks {
    uint32_t commas, quotes;
};

#ifdef __SSE2__
inline Marks marks16(const char* p) {
    const auto bytes = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
    return {uint32_t(_mm_movemask_epi8(_mm_cmpeq_epi8(bytes, _mm_set1_epi8(',')))),
            uint32_t(_mm_movemask_epi8(_mm_cmpeq_epi8(bytes, _mm_set1_epi8('"'))))};
}
#endif
#ifdef __AVX2__
inline Marks marks32(const char* p) {
    const auto bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
    return {uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8(',')))),
            uint32_t(_mm256_movemask_epi8(_mm256_cmpeq_epi8(bytes, _mm256_set1_epi8('"'))))};
}
#endif

#if defined(__AVX2__) && defined(__BMI2__)
struct WideMarks {
    uint64_t commas, quotes;
};

// Two possibly overlapping loads cover 32 through 64 bytes without reading
// beyond the line. Overlapping bytes set the same bits in both masks.
inline WideMarks marks32_to_64(const char* p, size_t n) {
    const auto first = marks32(p), last = marks32(p + n - 32);
    return {first.commas | (uint64_t(last.commas) << (n - 32)),
            first.quotes | (uint64_t(last.quotes) << (n - 32))};
}

// PEXT collects selected bits in byte order. Selecting the starts and ends of
// fields produces one bit per field: each quote at a start must pair with a
// distinct quote at the same field's end. Counting all quotes catches interior
// quotes too. Keep the matched bits to strip every quoted span without copying.
template<size_t N>
inline bool fast_fields(SV line, std::array<SV, N>& out) {
    const size_t n = line.size();
    const char* p = line.data();
    WideMarks low{}, high{};
    uint64_t openings, closings;
    unsigned quote_count;
    if (n <= 64) {
        low = marks32_to_64(p, n);
        if (__builtin_popcountll(low.commas) != N - 1) return false;
        const uint64_t starts = (low.commas << 1) | 1;
        const uint64_t ends = (low.commas >> 1) | (uint64_t{1} << (n - 1));
        if (low.quotes & starts & ends) return false; // a field consisting of one quote
        openings = _pext_u64(low.quotes, starts);
        closings = _pext_u64(low.quotes, ends);
        quote_count = __builtin_popcountll(low.quotes);
    } else {
        low = marks32_to_64(p, 64);
        if (n >= 96) {
            high = marks32_to_64(p + 64, n - 64);
        } else {
            const auto tail = marks32(p + n - 32);
            high = {tail.commas >> (96 - n), tail.quotes >> (96 - n)};
        }
        if (__builtin_popcountll(low.commas) + __builtin_popcountll(high.commas) != N - 1)
            return false;
        const uint64_t starts_low = (low.commas << 1) | 1;
        const uint64_t ends_low = (low.commas >> 1) | (high.commas << 63);
        const uint64_t starts_high = (high.commas << 1) | (low.commas >> 63);
        const uint64_t ends_high = (high.commas >> 1) | (uint64_t{1} << (n - 65));
        if ((low.quotes & starts_low & ends_low) || (high.quotes & starts_high & ends_high))
            return false;
        openings = _pext_u64(low.quotes, starts_low) |
                   (_pext_u64(high.quotes, starts_high) << __builtin_popcountll(starts_low));
        closings = _pext_u64(low.quotes, ends_low) |
                   (_pext_u64(high.quotes, ends_high) << __builtin_popcountll(ends_low));
        quote_count = __builtin_popcountll(low.quotes) + __builtin_popcountll(high.quotes);
    }
    // A leading comma has no preceding byte to sample for the first end bit.
    closings <<= (low.commas & 1);
    if (openings != closings || quote_count != 2 * unsigned(__builtin_popcountll(openings)))
        return false;

    size_t index = 0, start = 0;
    auto field = [&](size_t end) {
        const size_t trim = (openings >> index) & 1;
        out[index++] = SV(p + start + trim, end - start - 2 * trim);
        start = end + 1;
    };
    while (low.commas) {
        field(__builtin_ctzll(low.commas));
        low.commas &= low.commas - 1;
    }
    while (high.commas) {
        field(64 + __builtin_ctzll(high.commas));
        high.commas &= high.commas - 1;
    }
    field(n);
    return true;
}
#endif

inline unsigned pair(const char* p) {
    return unsigned(p[0] - '0') * 10 + unsigned(p[1] - '0');
}

inline bool digits2(const char* p) {
    return unsigned(p[0] - '0') < 10 && unsigned(p[1] - '0') < 10;
}

// Calendar starts are shared by all threads. The thirteenth entry is the start
// of the next January, so month length and the ordinal date share one lookup.
using Calendar = std::array<std::array<int32_t, 13>, 10000>;
inline const Calendar calendar = [] {
    Calendar c{};
    int32_t start = -719528; // 0000-01-01, using proleptic Gregorian years
    constexpr int lengths[] = {31,28,31,30,31,30,31,31,30,31,30,31};
    for (int y = 0; y < 10000; ++y) {
        bool leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
        for (int m = 0; m < 12; ++m) {
            c[y][m] = start;
            start += lengths[m] + (m == 1 && leap);
        }
        c[y][12] = start;
    }
    return c;
}();

inline bool iso_prefix(const char* p) {
#ifdef __SSE2__
    const auto bytes = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
    const auto lower = _mm_setr_epi8('0','0','0','0','-','0','0','-','0','0','T','0','0',':','0','0');
    const auto span = _mm_setr_epi8(9,9,9,9,0,9,9,0,9,9,0,9,9,0,9,9);
    // Unsigned subtraction wraps bytes below their minimum above the span.
    const auto excess = _mm_subs_epu8(_mm_sub_epi8(bytes, lower), span);
    return _mm_movemask_epi8(_mm_cmpeq_epi8(excess, _mm_setzero_si128())) == 65535
        && p[16] == ':' && digits2(p + 17);
#else
    return digits2(p) && digits2(p+2) && p[4]=='-' && digits2(p+5) && p[7]=='-' &&
           digits2(p+8) && p[10]=='T' && digits2(p+11) && p[13]==':' &&
           digits2(p+14) && p[16]==':' && digits2(p+17);
#endif
}

inline constexpr int64_t invalid = std::numeric_limits<int64_t>::min();

inline int64_t civil_ms(const char* p) {
    const unsigned y = pair(p) * 100 + pair(p + 2);
    const unsigned m = pair(p + 5), d = pair(p + 8);
    const unsigned h = pair(p + 11), minute = pair(p + 14), sec = pair(p + 17);
    if (y >= calendar.size() || m - 1 >= 12 || h >= 24 || minute >= 60 || sec >= 60)
        return invalid;
    const auto& months = calendar[y];
    const int32_t first = months[m - 1];
    if (d - 1 >= unsigned(months[m] - first)) return invalid;
    const int64_t day = int64_t(first) + d - 1;
    return day * 86400000 + (h * 3600 + minute * 60 + sec) * 1000;
}
} // namespace parse_detail

// A comma always separates fields. Count all quotes while locating commas,
// then account for a pair on each quoted field. Any leftover quote is illegal.
template<size_t N>
inline bool fields(SV line, std::array<SV, N>& out) {
#if defined(__AVX2__) && defined(__BMI2__)
    if (line.size() >= 32 && line.size() <= 128 && N < 32)
        return parse_detail::fast_fields(line,out);
#endif
    if (line.empty()) return false;
    const char* const data = line.data();
    size_t first = 0, count = 0, quotes = 0, paired = 0;
    auto field = [&](size_t end) {
        if (count == N) return false;
        const size_t len = end - first;
        const bool wrapped = len && data[first] == '"';
        if (wrapped && (len < 2 || data[end - 1] != '"')) return false;
        paired += 2 * size_t(wrapped);
        out[count++] = SV(data + first + wrapped, len - 2 * size_t(wrapped));
        first = end + 1;
        return true;
    };

    size_t i = 0;
#ifdef __SSE2__
    auto consume = [&](parse_detail::Marks masks, size_t base) {
        quotes += __builtin_popcount(masks.quotes);
        while (masks.commas) {
            const unsigned bit = __builtin_ctz(masks.commas);
            if (!field(base + bit)) return false;
            masks.commas &= masks.commas - 1;
        }
        return true;
    };
#ifdef __AVX2__
    while (line.size() - i >= 32) {
        if (!consume(parse_detail::marks32(data + i), i)) return false;
        i += 32;
    }
#endif
    while (line.size() - i >= 16) {
        if (!consume(parse_detail::marks16(data + i), i)) return false;
        i += 16;
    }
    if (line.size() >= 16 && i < line.size()) {
        // Overlap the previous block to keep the final load inside the view.
        auto tail = parse_detail::marks16(data + line.size() - 16);
        const unsigned shift = unsigned(i + 16 - line.size());
        if (!consume({tail.commas >> shift, tail.quotes >> shift}, i)) return false;
        i = line.size();
    }
#endif
    for (; i < line.size(); ++i) {
        if (data[i] == ',' && !field(i)) return false;
        quotes += data[i] == '"';
    }
    return field(line.size()) && count == N && quotes == paired;
}

inline int64_t ingest_time(SV s) {
    return s.size() == 20 ? parse_detail::civil_ms(s.data()) : parse_detail::invalid;
}

inline int64_t timestamp(SV s) {
    using namespace parse_detail;
    const size_t n = s.size();
    if (n == 0) return invalid;
    const char* p = s.data();
    if ((n == 20 || n == 25) && p[4] == '-') {
        if (!iso_prefix(p)) return invalid;
        if (n == 20) return p[19] == 'Z' ? civil_ms(p) : invalid;
        if ((p[19] != '+' && p[19] != '-') || p[22] != ':' ||
            !digits2(p + 20) || !digits2(p + 23)) return invalid;
        const unsigned h = pair(p + 20), m = pair(p + 23);
        if (h >= 24 || m >= 60) return invalid;
        const auto local = civil_ms(p);
        if (local == invalid) return invalid;
        const int offset = int(h * 60 + m) * 60000;
        return p[19] == '+' ? local - offset : local + offset;
    }
#ifdef __AVX2__
    // Thirteen digits are common for millisecond dates in the 21st century.
    // Right-align them into four groups of four decimal digits.
    if (n == 13) {
        const auto first = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(p));
        const auto last = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(p + 5));
        const auto raw = _mm_sub_epi8(_mm_unpacklo_epi64(first, last), _mm_set1_epi8('0'));
        const auto digits = _mm_shuffle_epi8(raw, _mm_setr_epi8(-1,-1,-1,0,1,2,3,4,8,9,10,11,12,13,14,15));
        const auto excess = _mm_subs_epu8(digits, _mm_set1_epi8(9));
        if (!_mm_testz_si128(excess, excess)) return invalid;
        const auto tens = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a));
        const auto thousands = _mm_madd_epi16(tens, _mm_set1_epi32(0x00010064));
        const auto halves = _mm_madd_epi16(_mm_packs_epi32(thousands, thousands), _mm_set1_epi32(0x00012710));
        return int64_t(_mm_cvtsi128_si32(halves)) * 100000000 + _mm_extract_epi32(halves, 1);
    }
#endif
    if (unsigned(p[0] - '0') >= 10) return invalid;
    if (n <= 18) {
        int64_t v = 0;
        for (const char ch : s) {
            const unsigned digit = unsigned(ch - '0');
            if (digit >= 10) return invalid;
            v = 10 * v + digit;
        }
        return v;
    }
    int64_t v = 0;
    const auto result = std::from_chars(p, p+n, v);
    if (result.ptr != p+n) return invalid;
    // Any larger all-digit time is beyond every supported ISO window.
    return result.ec == std::errc{} ? v : std::numeric_limits<int64_t>::max();
}

inline int32_t price(SV s) {
    int32_t v = 0;
    for (const char ch : s) {
        const unsigned digit = unsigned(ch - '0');
        if (digit >= 10) return 0;
        v = 10 * v + int32_t(digit);
        if (v > 10000000) return 0;
    }
    return v;
}

} // namespace ss
