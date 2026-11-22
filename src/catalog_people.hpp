#pragma once

#include "arena.hpp"
#include "io.hpp"
#include "keys.hpp"
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <immintrin.h>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <unordered_map>
#include <vector>

namespace blitz {

// Dense-prefix or hash-fallback catalog lookup (products -> category, campaigns -> channel).
class CompactIndex {
    std::string prefix_;
    std::vector<uint32_t> direct_;
    std::vector<uint8_t> direct_u8_;
    bool use_u8_ = false;
    std::unordered_map<std::string, uint32_t> fallback_;
    static constexpr uint32_t MISSING = UINT32_MAX;

    inline bool parse_suffix(SV id, uint32_t& out) const {
        const size_t pfx_len = prefix_.size();
        if (id.size() <= pfx_len) return false;
        if (pfx_len > 0 && std::memcmp(id.data(), prefix_.data(), pfx_len) != 0) return false;
        const char* p = id.data() + pfx_len;
        const size_t rem = id.size() - pfx_len;
        if (rem > 1 && p[0] == '0') return false;
        if (rem > 9) return false;
        uint32_t v = 0;
        for (size_t i = 0; i < rem; ++i) {
            unsigned d = unsigned(p[i] - '0');
            if (d >= 10u) return false;
            v = v * 10u + d;
        }
        out = v;
        return true;
    }

public:
    bool is_c_underscore_dense = false;

    void build(const std::vector<std::pair<std::string, uint32_t>>& rows, uint32_t fallback_val) {
        if (rows.empty()) return;
        const std::string& sample = rows.front().first;
        size_t pfx = sample.size();
        while (pfx > 0 && sample[pfx - 1] >= '0' && sample[pfx - 1] <= '9') --pfx;
        prefix_ = sample.substr(0, pfx);
        uint32_t high = 0;
        bool dense = true;
        for (const auto& [id, val] : rows) {
            uint32_t num = 0;
            if (!parse_suffix(id, num) || num > rows.size() * 3 + 16) {
                dense = false;
                break;
            }
            if (num > high) high = num;
        }
        if (dense) {
            is_c_underscore_dense = (prefix_ == "c_");
            uint32_t max_v = fallback_val;
            for (const auto& [id, val] : rows)
                if (val > max_v) max_v = val;
            if (max_v < 255) {
                use_u8_ = true;
                direct_u8_.assign(size_t(high) + 1, uint8_t(fallback_val));
                for (const auto& [id, val] : rows) {
                    uint32_t num = 0;
                    parse_suffix(id, num);
                    direct_u8_[num] = uint8_t(val);
                }
            } else {
                use_u8_ = false;
                direct_.assign(size_t(high) + 1, MISSING);
                for (const auto& [id, val] : rows) {
                    uint32_t num = 0;
                    parse_suffix(id, num);
                    direct_[num] = val;
                }
            }
        } else {
            fallback_.reserve(rows.size() * 2);
            for (const auto& [id, val] : rows) fallback_.emplace(id, val);
        }
    }

    inline uint32_t lookup_direct_num(uint32_t num, uint32_t fallback_val) const {
        if (use_u8_) {
            return (num < direct_u8_.size()) ? direct_u8_[num] : fallback_val;
        }
        if (num >= direct_.size()) return fallback_val;
        uint32_t v = direct_[num];
        return v == MISSING ? fallback_val : v;
    }

    inline uint32_t lookup(SV id, uint32_t fallback_val) const {
        if (__builtin_expect(use_u8_, 1)) {
            uint32_t num = 0;
            if (__builtin_expect(!parse_suffix(id, num) || num >= direct_u8_.size(), 0))
                return fallback_val;
            return direct_u8_[num];
        }
        if (!direct_.empty()) {
            uint32_t num = 0;
            if (__builtin_expect(!parse_suffix(id, num) || num >= direct_.size(), 0))
                return fallback_val;
            uint32_t v = direct_[num];
            return v == MISSING ? fallback_val : v;
        }
        auto it = fallback_.find(std::string(id));
        return it == fallback_.end() ? fallback_val : it->second;
    }
};

class Catalog {
    CompactIndex products_;
    CompactIndex campaigns_idx_;
    uint32_t unknown_cat_ = 0;

public:
    std::vector<std::string> categories;
    std::vector<std::string> campaigns{"organic"};
    std::vector<std::string> channels{"organic"};

    explicit Catalog(const fs::path& data_dir) {
        std::vector<std::pair<std::string, std::string>> prod_raw;
        std::set<std::string> distinct_cats{"unknown"};
        {
            PreadInput prod_in(data_dir / "products.csv");
            prod_in.scan<2>(1, [&](unsigned, const std::array<SV, 2>& f) {
                prod_raw.emplace_back(std::string(f[0]), std::string(f[1]));
                distinct_cats.emplace(f[1]);
            }, [](unsigned) {});
        }
        categories.reserve(distinct_cats.size());
        std::unordered_map<std::string, uint32_t> cat_ranks;
        cat_ranks.reserve(distinct_cats.size() * 2);
        for (const auto& c : distinct_cats) {
            cat_ranks.emplace(c, uint32_t(categories.size()));
            categories.push_back(c);
        }
        unknown_cat_ = cat_ranks.at("unknown");
        std::vector<std::pair<std::string, uint32_t>> prod_rows;
        prod_rows.reserve(prod_raw.size());
        for (auto& [pid, cname] : prod_raw) {
            prod_rows.emplace_back(std::move(pid), cat_ranks.at(cname));
        }
        products_.build(prod_rows, unknown_cat_);

        std::vector<std::pair<std::string, uint32_t>> camp_rows;
        {
            PreadInput camp_in(data_dir / "campaigns.csv");
            camp_in.scan<2>(1, [&](unsigned, const std::array<SV, 2>& f) {
                uint32_t cid = uint32_t(campaigns.size());
                camp_rows.emplace_back(std::string(f[0]), cid);
                campaigns.emplace_back(f[0]);
                channels.emplace_back(f[1]);
            }, [](unsigned) {});
        }
        campaigns_idx_.build(camp_rows, 0);
    }

    inline bool is_fast_campaigns() const { return campaigns_idx_.is_c_underscore_dense; }
    inline uint32_t campaign_by_num(uint32_t num) const { return campaigns_idx_.lookup_direct_num(num, 0); }
    inline uint32_t category(SV product_id) const { return products_.lookup(product_id, unknown_cat_); }
    inline uint32_t campaign(SV campaign_id) const { return campaigns_idx_.lookup(campaign_id, 0); }
};

// Streamlined lock-free Linked IDs resolver:
// 1. 6-instruction raw-ASCII dual-ID link CSV parser (ZERO base-36 math!).
// 2. Single-pass parallel lock-free intern + union-find on mmap-backed table
//    (ZERO 512-vector intermediate scatter, 1-instruction bswap64 bytewise compare!).
// 3. Parallel Blocked Bloom Filter + Compact Non-Canonical Alias Map.
class People {
    struct Slot {
        uint64_t key;
        uint64_t canonical;
    };

    bool has_aliases_ = false;
    std::vector<uint64_t> bloom_;
    size_t bloom_mask_ = 0;
    Slot* slots_ = nullptr;
    size_t slot_mask_ = 0;

public:
    People(const fs::path& data_dir, RawKeys<'u'>& user_keys, unsigned threads) {
        PreadInput links_in(data_dir / "links", 1u << 20);
        if (links_in.total_bytes == 0) return;

        struct RawEdge {
            uint64_t a, b;
        };
        std::vector<std::vector<RawEdge>> w_edges(threads);
        const size_t est = links_in.total_bytes / (22 * std::max(1u, threads)) + 64;
        for (auto& v : w_edges) v.reserve(est);

        // 6-instruction raw-ASCII dual-ID link parser!
        links_in.scan_buffers(threads, [&](unsigned w, const char* buf, size_t scan_len) {
            auto& out = w_edges[w];
            const char* cur = buf;
            const char* end = buf + scan_len;
            const __m128i c_lo = _mm_set1_epi8(0x30);
            const __m128i c_span = _mm_set1_epi8(74); // [0x30..0x7A]
            std::array<SV, 2> f;

            while (cur < end) {
                const char* line_start = cur;
                const size_t q0 = (*cur == '"');
                const char* s0 = cur + q0;
                const char* p1 = s0 + 10 + q0;
                const size_t q1 = (*p1 == '"');
                const char* s1 = p1 + q1;
                bool both9 = (s0[0] == 'u') & (s0[9] == (q0 ? '"' : ',')) & (!q0 | (s0[10] == ',')) &
                             (s1[0] == 'u') & (s1[9] == (q1 ? '"' : '\n')) & (!q1 | (s1[10] == '\n'));
                if (__builtin_expect(both9, 1)) {
                    uint64_t ka, kb;
                    std::memcpy(&ka, s0 + 1, 8);
                    std::memcpy(&kb, s1 + 1, 8);
                    __m128i v = _mm_set_epi64x(int64_t(kb), int64_t(ka));
                    __m128i bad = _mm_subs_epu8(_mm_sub_epi8(v, c_lo), c_span);
                    if (__builtin_expect(_mm_testz_si128(bad, bad), 1)) {
                        if (__builtin_expect(ka != kb, 1)) out.push_back({ka, kb});
                        cur = s1 + 10 + q1;
                        continue;
                    }
                }
                const char* nl = static_cast<const char*>(std::memchr(line_start, '\n', size_t(end - line_start)));
                size_t llen = nl ? size_t(nl - line_start) : size_t(end - line_start);
                cur = nl ? (nl + 1) : end;
                if (split_fields_slow<2>(SV(line_start, llen), f) && !f[0].empty() && !f[1].empty()) {
                    uint64_t a = user_keys.encode(f[0]);
                    uint64_t b = user_keys.encode(f[1]);
                    if (a != b) out.push_back({a, b});
                }
            }
        }, [](unsigned) {});

        size_t total_edges = 0;
        for (const auto& v : w_edges) total_edges += v.size();
        if (total_edges == 0) return;

        // Direct lock-free vertex table + DSU (guaranteed load <= 75% even for disjoint edges!)
        size_t tcap = 16;
        while (tcap - tcap / 4 < 2 * total_edges) tcap *= 2;
        if (tcap > UINT32_MAX) throw std::length_error("Too many link endpoints");
        const size_t tmask = tcap - 1;

        auto* t_key = static_cast<std::atomic<uint64_t>*>(
            ::mmap(nullptr, tcap * sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
        auto* parent = static_cast<std::atomic<uint32_t>*>(
            ::mmap(nullptr, tcap * sizeof(uint32_t), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
#ifdef MADV_HUGEPAGE
        if (safe_for_thp()) {
            if (tcap * sizeof(uint64_t) >= (2u << 20)) ::madvise(t_key, tcap * sizeof(uint64_t), MADV_HUGEPAGE);
            if (tcap * sizeof(uint32_t) >= (2u << 20)) ::madvise(parent, tcap * sizeof(uint32_t), MADV_HUGEPAGE);
        }
#endif

        auto intern = [&](uint64_t k) __attribute__((always_inline)) -> uint32_t {
            size_t pos = size_t(fast_hash(k)) & tmask;
            for (;;) {
                uint64_t cur = t_key[pos].load(std::memory_order_acquire);
                if (cur == k) return uint32_t(pos);
                if (cur == 0) {
                    uint64_t exp = 0;
                    if (t_key[pos].compare_exchange_strong(exp, k, std::memory_order_acq_rel) || exp == k)
                        return uint32_t(pos);
                }
                pos = (pos + 1) & tmask;
            }
        };

        auto find_root = [&](uint32_t x) __attribute__((always_inline)) -> uint32_t {
            while (uint32_t p = parent[x].load(std::memory_order_acquire)) {
                uint32_t pn = p - 1;
                if (uint32_t g = parent[pn].load(std::memory_order_acquire)) {
                    parent[x].store(g, std::memory_order_release);
                }
                x = pn;
            }
            return x;
        };

        struct Batch { const RawEdge* beg; const RawEdge* end; };
        std::vector<Batch> batches;
        constexpr size_t BSIZE = 4096;
        for (const auto& v : w_edges) {
            for (size_t i = 0; i < v.size(); i += BSIZE)
                batches.push_back({v.data() + i, v.data() + std::min(v.size(), i + BSIZE)});
        }

        parallel_for(batches.size(), threads, [&](unsigned, size_t bi) {
            const RawEdge* beg = batches[bi].beg;
            const RawEdge* end = batches[bi].end;
            for (const RawEdge* e = beg; e != end; ++e) {
                if (e + 16 < end) {
                    _mm_prefetch(reinterpret_cast<const char*>(&t_key[size_t(fast_hash((e + 16)->a)) & tmask]),
                                 _MM_HINT_T0);
                    _mm_prefetch(reinterpret_cast<const char*>(&t_key[size_t(fast_hash((e + 16)->b)) & tmask]),
                                 _MM_HINT_T0);
                }
                uint32_t a = intern(e->a);
                uint32_t b = intern(e->b);
                for (;;) {
                    a = find_root(a);
                    b = find_root(b);
                    if (a == b) break;
                    uint64_t ka = t_key[a].load(std::memory_order_relaxed);
                    uint64_t kb = t_key[b].load(std::memory_order_relaxed);
                    if (user_keys.less(kb, ka)) std::swap(a, b);
                    uint32_t exp = 0;
                    if (parent[b].compare_exchange_strong(exp, a + 1, std::memory_order_acq_rel)) break;
                }
            }
        });

        // Parallel path compression + non-canonical count!
        constexpr size_t VSTEP = 8192;
        const size_t nblocks = (tcap + VSTEP - 1) / VSTEP;
        std::atomic<size_t> non_canon_atomic{0};
        parallel_for(nblocks, threads, [&](unsigned, size_t bi) {
            const size_t lo = bi * VSTEP;
            const size_t hi = std::min(tcap, lo + VSTEP);
            size_t local_nc = 0;
            for (size_t i = lo; i < hi; ++i) {
                if (t_key[i].load(std::memory_order_relaxed) == 0) continue;
                uint32_t r = find_root(uint32_t(i));
                if (r != uint32_t(i)) {
                    parent[i].store(r + 1, std::memory_order_release);
                    ++local_nc;
                }
            }
            non_canon_atomic.fetch_add(local_nc, std::memory_order_relaxed);
        });

        const size_t non_canon = non_canon_atomic.load(std::memory_order_relaxed);
        if (non_canon == 0) return;

        // Scaled Blocked Bloom Filter (1 MiB at 20M, 4 MiB at 60M => <1% FP rate!)
        size_t bwords = 1024;
        while (bwords * 64 < non_canon * 18 && bwords < 524288) bwords *= 2;
        bloom_.assign(bwords, 0);
        bloom_mask_ = bwords - 1;
        auto* bloom_atom = reinterpret_cast<std::atomic<uint64_t>*>(bloom_.data());

        // Compact Non-Canonical Alias Table (<= 60% load factor) populated in parallel!
        size_t cap = 16;
        while (cap * 6 < non_canon * 10) cap *= 2;
        slot_mask_ = cap - 1;
        slots_ = static_cast<Slot*>(::mmap(nullptr, cap * sizeof(Slot),
                                           PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
#ifdef MADV_HUGEPAGE
        if (safe_for_thp() && cap * sizeof(Slot) >= (2u << 20)) {
            ::madvise(slots_, cap * sizeof(Slot), MADV_HUGEPAGE);
        }
#endif

        parallel_for(nblocks, threads, [&](unsigned, size_t bi) {
            const size_t lo = bi * VSTEP;
            const size_t hi = std::min(tcap, lo + VSTEP);
            for (size_t i = lo; i < hi; ++i) {
                uint32_t p = parent[i].load(std::memory_order_relaxed);
                if (p == 0) continue;
                uint64_t k = t_key[i].load(std::memory_order_relaxed);
                uint64_t canon = t_key[p - 1].load(std::memory_order_relaxed);
                uint64_t h = fast_hash(k);
                uint64_t bits = (uint64_t{1} << ((h >> 32) & 63)) |
                                (uint64_t{1} << ((h >> 38) & 63)) |
                                (uint64_t{1} << ((h >> 44) & 63));
                bloom_atom[h & bloom_mask_].fetch_or(bits, std::memory_order_relaxed);

                size_t pos = size_t(h) & slot_mask_;
                for (;;) {
                    auto* slot_k = reinterpret_cast<std::atomic<uint64_t>*>(&slots_[pos].key);
                    uint64_t exp = 0;
                    if (slot_k->load(std::memory_order_relaxed) == 0 &&
                        slot_k->compare_exchange_strong(exp, k, std::memory_order_relaxed)) {
                        slots_[pos].canonical = canon;
                        break;
                    }
                    pos = (pos + 1) & slot_mask_;
                }
            }
        });
        has_aliases_ = true;
    }

    inline bool has_aliases() const { return has_aliases_; }

    inline void prefetch_bloom_h(uint64_t h) const {
        if (__builtin_expect(has_aliases_, 1)) {
            _mm_prefetch(reinterpret_cast<const char*>(&bloom_[h & bloom_mask_]), _MM_HINT_T0);
        }
    }

    inline bool check_and_prefetch_h(uint64_t h) const {
        if (__builtin_expect(!has_aliases_, 0)) return false;
        const uint64_t w = bloom_[h & bloom_mask_];
        const uint64_t bits = (uint64_t{1} << ((h >> 32) & 63)) |
                              (uint64_t{1} << ((h >> 38) & 63)) |
                              (uint64_t{1} << ((h >> 44) & 63));
        if ((w & bits) != bits) return false;
        _mm_prefetch(reinterpret_cast<const char*>(&slots_[h & slot_mask_]), _MM_HINT_T0);
        return true;
    }

    inline bool check_and_prefetch(uint64_t key, uint64_t& out_h) const {
        if (__builtin_expect(!has_aliases_, 0)) return false;
        const uint64_t h = fast_hash(key);
        out_h = h;
        return check_and_prefetch_h(h);
    }

    inline uint64_t lookup_hashed(uint64_t key, uint64_t h) const {
        size_t pos = size_t(h) & slot_mask_;
        for (;;) {
            const uint64_t k = slots_[pos].key;
            if (k == key) return slots_[pos].canonical;
            if (k == 0) return key;
            pos = (pos + 1) & slot_mask_;
        }
    }

    inline uint64_t resolve(uint64_t key) const {
        uint64_t h;
        if (!check_and_prefetch(key, h)) return key;
        return lookup_hashed(key, h);
    }
};

} // namespace blitz
