#pragma once

#include <cstdint>
#include <cstring>
#include <deque>
#include <immintrin.h>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>

namespace blitz {

using SV = std::string_view;

// Fast 1-multiply 128-bit wyhash mixer (4 instructions, 4 cycles, full 64-bit avalanche!).
// Guarantees statistical independence across all bit slices (bucket, Bloom, HT, shard).
inline uint64_t fast_hash(uint64_t x) {
    __uint128_t r = __uint128_t(x ^ 0xa0761d6478bd642fULL) * __uint128_t(x ^ 0xe7037ed1a0b428dbULL);
    return uint64_t(r) ^ uint64_t(r >> 64);
}

inline uint64_t mix64(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

// Zero-conversion raw 64-bit ASCII key encoder for IDs with common prefix ('e' or 'u'):
// - Fast domain (bit 63 == 0): strings of length 2..9 starting with `Prefix` whose
//   remaining 1..8 bytes are non-NUL ASCII (1..127). Stored verbatim in little-endian
//   `uint64_t` (zero-padded in high bytes). ZERO base-36 math, 3-instruction decode,
//   1-instruction bytewise comparison via bswap64!
// - Fallback domain (bit 63 == 1): exact mutex-protected interner for all other strings
//   (uppercase prefix, length 1 or >9, non-ASCII UTF-8, embedded NUL, etc.).
template <char Prefix>
class RawKeys {
public:
    static constexpr uint64_t HIGH = uint64_t{1} << 63;

private:
    mutable std::mutex mu_;
    std::deque<std::string> storage_;
    std::unordered_map<SV, uint64_t> intern_;

    uint64_t intern_slow(SV s) {
        std::lock_guard lk(mu_);
        auto it = intern_.find(s);
        if (it != intern_.end()) return it->second;
        uint64_t key = HIGH | storage_.size();
        storage_.emplace_back(s);
        intern_.emplace(storage_.back(), key);
        return key;
    }

public:
    inline uint64_t encode(SV s) {
        const size_t n = s.size();
        const char* p = s.data();
        if (__builtin_expect(n >= 2 && n <= 9 && p[0] == Prefix, 1)) {
            const size_t tlen = n - 1;
            uint64_t word = 0;
            uint64_t mask;
            if (tlen == 8) {
                std::memcpy(&word, p + 1, 8);
                mask = ~uint64_t{0};
            } else {
                std::memcpy(&word, p + 1, tlen);
                mask = (uint64_t{1} << (tlen * 8)) - 1;
            }
            const uint64_t zero_bytes = (word - 0x0101010101010101ULL) & ~word & mask;
            if (__builtin_expect(((zero_bytes | word) & 0x8080808080808080ULL) == 0, 1)) {
                return word;
            }
        }
        return intern_slow(s);
    }

    // 3-instruction zero-allocation decode into caller's 9-byte stack buffer!
    inline SV decode(uint64_t key, char buf[9]) const {
        if (__builtin_expect((key & HIGH) != 0, 0)) {
            return storage_[key ^ HIGH];
        }
        buf[0] = Prefix;
        std::memcpy(buf + 1, &key, 8);
        const size_t tlen = 8u - (size_t(__builtin_clzll(key)) >> 3);
        return SV(buf, 1 + tlen);
    }

    // 1-instruction bytewise lexicographical compare for fast keys!
    inline bool less(uint64_t a, uint64_t b) const {
        if (__builtin_expect(((a | b) & HIGH) == 0, 1)) {
            return __builtin_bswap64(a) < __builtin_bswap64(b);
        }
        char ba[9], bb[9];
        return decode(a, ba) < decode(b, bb);
    }
};

// Zero-SWAR 1-bswap order-preserving 64-bit click_id encoder!
// Fast domain (bit 63 == 0): 'k' + 1..8 non-NUL ASCII bytes, stored big-endian via bswap64
// so integer `<` IS exact bytewise order. Fallback (bit 63 == 1): exact interner.
class ClickKeys {
    static constexpr uint64_t HIGH = uint64_t{1} << 63;
    mutable std::mutex mu_;
    std::deque<std::string> storage_;
    std::unordered_map<SV, uint64_t> intern_;

    uint64_t intern_slow(SV s) {
        std::lock_guard lk(mu_);
        auto it = intern_.find(s);
        if (it != intern_.end()) return it->second;
        uint64_t key = HIGH | storage_.size();
        storage_.emplace_back(s);
        intern_.emplace(storage_.back(), key);
        return key;
    }

public:
    inline uint64_t encode(SV s) {
        const size_t n = s.size();
        const char* p = s.data();
        if (__builtin_expect(n >= 2 && n <= 9 && p[0] == 'k', 1)) {
            const size_t tlen = n - 1;
            uint64_t word = 0;
            uint64_t mask;
            if (tlen == 8) {
                std::memcpy(&word, p + 1, 8);
                mask = ~uint64_t{0};
            } else {
                std::memcpy(&word, p + 1, tlen);
                mask = (uint64_t{1} << (tlen * 8)) - 1;
            }
            const uint64_t zero_bytes = (word - 0x0101010101010101ULL) & ~word & mask;
            if (__builtin_expect(((zero_bytes | word) & 0x8080808080808080ULL) == 0, 1)) {
                return __builtin_bswap64(word);
            }
        }
        return intern_slow(s);
    }

    inline SV decode(uint64_t key, char buf[9]) const {
        if (__builtin_expect((key & HIGH) != 0, 0)) {
            return storage_[key ^ HIGH];
        }
        buf[0] = 'k';
        uint64_t le = __builtin_bswap64(key);
        std::memcpy(buf + 1, &le, 8);
        const size_t tlen = 8u - (size_t(__builtin_clzll(le)) >> 3);
        return SV(buf, 1 + tlen);
    }

    inline bool less(uint64_t a, uint64_t b) const {
        if (__builtin_expect(((a | b) & HIGH) == 0, 1)) return a < b;
        char ba[9], bb[9];
        return decode(a, ba) < decode(b, bb);
    }
};

} // namespace blitz
