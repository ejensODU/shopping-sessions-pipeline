#pragma once
#include <cstdint>
#include <cstring>
#include <deque>
#include <immintrin.h>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace ss {

// Short non-NUL ASCII IDs fit in nine seven-bit digits. Zero padding makes
// the numeric ordering identical to the original byte ordering. All other IDs
// are interned with their complete bytes in a disjoint range.
class Keys {
    static constexpr uint64_t high = uint64_t{1} << 63;
    static constexpr uint64_t low7 = 0x7f7f7f7f7f7f7f7fULL;
    std::mutex mutex_;
    std::deque<std::string> storage_;
    std::unordered_map<std::string_view, uint64_t> other_;
public:
    uint64_t encode(std::string_view s) {
        const size_t n = s.size();
        if (n && n <= 9) {
            uint64_t word = 0;
            bool clean;
            if (n >= 8) {
                std::memcpy(&word, s.data(), 8);
                const uint64_t zero_bytes = (word - 0x0101010101010101ULL) & ~word;
                clean = ((zero_bytes | word) & ~low7) == 0;
            } else {
                std::memcpy(&word, s.data(), n);
                clean = !(word & ~low7) && !std::memchr(s.data(), 0, n);
            }
            const unsigned tail = n == 9 ? static_cast<unsigned char>(s[8]) : 0;
            if (clean && (n != 9 || (tail && tail < 128))) {
                return (_pext_u64(__builtin_bswap64(word), low7) << 7) | tail;
            }
        }
        std::lock_guard guard(mutex_);
        auto it = other_.find(s);
        if (it != other_.end()) return it->second;
        const uint64_t key = high | storage_.size();
        storage_.emplace_back(s);
        other_.emplace(storage_.back(), key);
        return key;
    }

    std::string decode(uint64_t key) {
        if (key & high) {
            std::lock_guard guard(mutex_);
            return storage_.at(key ^ high);
        }
        char bytes[9];
        int length = 0;
        for (int shift = 56; shift >= 0; shift -= 7) {
            const char ch = static_cast<char>((key >> shift) & 127);
            if (!ch) break;
            bytes[length++] = ch;
        }
        return {bytes, static_cast<size_t>(length)};
    }

    bool less(uint64_t a, uint64_t b) {
        return ((a | b) & high) ? decode(a) < decode(b) : a < b;
    }
};

inline uint64_t mix(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

// The caller uses nonempty, nonzero keys. Slots stay below 75% occupancy.
template<class Value>
class KeyMap {
    std::vector<uint64_t> keys_;
    std::vector<Value> values_;
    size_t used_ = 0;

    size_t slot(uint64_t key) const {
        const size_t mask = keys_.size() - 1;
        size_t i = mix(key) & mask;
        while (keys_[i] && keys_[i] != key) i = (i + 1) & mask;
        return i;
    }
public:
    explicit KeyMap(size_t expected = 0) { reserve(expected); }
    void reserve(size_t expected) {
        size_t cap = 16;
        while (cap - cap / 4 < expected) cap *= 2;
        if (cap <= keys_.size()) return;
        auto keys = std::move(keys_);
        auto values = std::move(values_);
        keys_.assign(cap, 0);
        values_.resize(cap);
        for (size_t j = 0; j < keys.size(); ++j) if (keys[j]) {
            const auto i = slot(keys[j]);
            keys_[i] = keys[j];
            values_[i] = values[j];
        }
    }
    Value* find(uint64_t key) {
        const size_t i = slot(key);
        return keys_[i] ? &values_[i] : nullptr;
    }
    const Value* find(uint64_t key) const {
        const size_t i = slot(key);
        return keys_[i] ? &values_[i] : nullptr;
    }
    std::pair<Value*, bool> insert(uint64_t key, Value value) {
        if (used_ >= keys_.size() - keys_.size() / 4) reserve(used_ + 1);
        size_t i = slot(key);
        if (keys_[i]) return {&values_[i], false};
        keys_[i] = key;
        values_[i] = value;
        ++used_;
        return {&values_[i], true};
    }
};
} // namespace ss
