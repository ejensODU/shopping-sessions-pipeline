#pragma once

#include "input.hpp"
#include "keys.hpp"
#include "parse.hpp"
#include <charconv>
#include <limits>
#include <set>

namespace ss {

// Slot keys are assigned once. During the build, parents always point toward
// a smaller key. Concurrent unions can therefore attach different trees
// without a lock or cycle, even when many people share an ID.
class People {
    struct Slot {
        std::atomic<uint64_t> key{0};
        uint64_t canonical = 0;
    };
    std::unique_ptr<Slot[]> table_;
    size_t mask_;

    size_t lookup(uint64_t key) const {
        size_t i = mix(key) & mask_;
        for (;;) {
            const auto current = table_[i].key.load(std::memory_order_acquire);
            if (!current || current == key) return i;
            i = (i + 1) & mask_;
        }
    }

    size_t intern(uint64_t key) {
        for (;;) {
            const auto i = lookup(key);
            uint64_t expected = 0;
            if (table_[i].key.load(std::memory_order_acquire) == key ||
                table_[i].key.compare_exchange_strong(expected, key, std::memory_order_acq_rel) ||
                expected == key) return i;
        }
    }
public:
    People(const fs::path& data_dir, Keys& keys, unsigned threads = 8) {
        struct Edge { uint64_t a, b; };
        std::vector<std::vector<Edge>> edges(threads);
        Input links(data_dir / "links");
        for (auto& e : edges) e.reserve(links.bytes / (22 * threads));
        links.read(threads, false, [&](unsigned w, SV line) {
            std::array<SV, 2> pair;
            if (!fields(line, pair) || pair[0].empty() || pair[1].empty()) return;
            edges[w].push_back({keys.encode(pair[0]), keys.encode(pair[1])});
        });
        size_t count = 0;
        for (const auto& e : edges) count += e.size();
        size_t capacity = 16;
        while (capacity - capacity / 8 < 2 * count) capacity *= 2;
        if (capacity > UINT32_MAX) throw std::length_error("Too many link endpoints");
        mask_ = capacity - 1;
        table_ = std::make_unique<Slot[]>(capacity);

        auto parent = std::make_unique<std::atomic<uint32_t>[]>(capacity);
        for (size_t i = 0; i < capacity; ++i) parent[i].store(0, std::memory_order_relaxed);
        auto root = [&](uint32_t i) {
            while (uint32_t p = parent[i].load(std::memory_order_acquire)) {
                --p;
                if (uint32_t grand = parent[p].load(std::memory_order_acquire))
                    parent[i].store(grand, std::memory_order_release);
                i = p;
            }
            return i;
        };
        struct Batch { const Edge* begin; const Edge* end; };
        std::vector<Batch> batches;
        constexpr size_t batch_size = 4096;
        for (auto& vector : edges) {
            for (size_t i = 0; i < vector.size(); i += batch_size)
                batches.push_back({vector.data() + i, vector.data() + std::min(vector.size(), i + batch_size)});
        }
        parallel(batches.size(), threads, [&](unsigned, size_t batch) {
            for (auto e = batches[batch].begin; e != batches[batch].end; ++e) {
                auto a = static_cast<uint32_t>(intern(e->a));
                auto b = static_cast<uint32_t>(intern(e->b));
                for (;;) {
                    a = root(a);
                    b = root(b);
                    if (a == b) break;
                    const auto ka = table_[a].key.load(std::memory_order_relaxed);
                    const auto kb = table_[b].key.load(std::memory_order_relaxed);
                    if (keys.less(ka, kb)) std::swap(a, b);
                    uint32_t expected = 0;
                    if (parent[a].compare_exchange_strong(expected, b + 1, std::memory_order_acq_rel)) break;
                }
            }
        });
        parallel((capacity + batch_size - 1) / batch_size, threads, [&](unsigned, size_t batch) {
            const size_t end = std::min(capacity, (batch + 1) * batch_size);
            for (size_t i = batch * batch_size; i < end; ++i) {
                if (table_[i].key.load(std::memory_order_relaxed))
                    table_[i].canonical = table_[root(i)].key.load(std::memory_order_relaxed);
            }
        });
    }

    uint64_t resolve(uint64_t key) const {
        const auto canonical = table_[lookup(key)].canonical;
        return canonical ? canonical : key;
    }
};

namespace catalog_detail {
// A catalog with a shared prefix and unpadded decimal suffixes can be indexed
// without hashing. Verify the property across the complete input, including a
// density bound. All other catalogs use full byte-string keys.
class Index {
    SV prefix_;
    std::vector<uint32_t> direct_;
    std::unordered_map<SV, uint32_t> by_id_;
    static constexpr uint32_t missing = UINT32_MAX;

    bool suffix(SV id, uint32_t& value) const {
        if (id.size() <= prefix_.size() || id.compare(0, prefix_.size(), prefix_) != 0) return false;
        id.remove_prefix(prefix_.size());
        if (id.size() > 1 && id[0] == '0') return false;
        auto end = id.data() + id.size();
        const auto r = std::from_chars(id.data(), end, value);
        return r.ec == std::errc{} && r.ptr == end;
    }

public:
    void build(const std::vector<std::pair<SV, uint32_t>>& rows) {
        if (rows.empty()) return;
        const SV sample = rows.front().first;
        size_t prefix_size = sample.size();
        while (prefix_size && sample[prefix_size - 1] >= '0' && sample[prefix_size - 1] <= '9') --prefix_size;
        prefix_ = sample.substr(0, prefix_size);
        uint32_t high = 0;
        bool dense = true;
        for (const auto& [id, value] : rows) {
            uint32_t number;
            if (!suffix(id, number) || number > rows.size() * 3) { dense = false; break; }
            high = std::max(high, number);
        }
        if (dense) {
            direct_.assign(size_t(high) + 1, missing);
            for (const auto& [id, value] : rows) {
                uint32_t number = 0;
                suffix(id, number);
                direct_[number] = value;
            }
        } else {
            by_id_.reserve(rows.size());
            for (const auto& [id, value] : rows) by_id_.emplace(id, value);
        }
    }

    uint32_t lookup(SV id, uint32_t fallback) const {
        if (direct_.empty()) {
            const auto i = by_id_.find(id);
            return i == by_id_.end() ? fallback : i->second;
        }
        uint32_t number;
        if (!suffix(id, number) || number >= direct_.size() || direct_[number] == missing) return fallback;
        return direct_[number];
    }
};
}  // namespace catalog_detail

class Catalog {
    Input product_file_, campaign_file_; // back the catalog index string views
    catalog_detail::Index products_, sources_;
    uint32_t unknown_;

public:
    std::vector<std::string> categories, campaigns{"organic"}, channels{"organic"};

    explicit Catalog(const fs::path& data_dir)
        : product_file_(data_dir / "products.csv"), campaign_file_(data_dir / "campaigns.csv") {
        std::vector<std::pair<SV, SV>> products;
        std::set<SV> distinct{"unknown"};
        product_file_.read(1, true, [&](unsigned, SV line) {
            std::array<SV, 2> row;
            if (!fields(line, row)) throw std::runtime_error("Malformed products.csv row");
            products.emplace_back(row[0], row[1]);
            distinct.insert(row[1]);
        });
        for (SV name : distinct) categories.emplace_back(name);
        std::unordered_map<SV, uint32_t> ranks;
        for (uint32_t i = 0; i < categories.size(); ++i) ranks.emplace(categories[i], i);
        unknown_ = ranks.at("unknown");
        std::vector<std::pair<SV, uint32_t>> rows;
        rows.reserve(products.size());
        for (const auto& [id, name] : products) rows.emplace_back(id, ranks.at(name));
        products_.build(rows);
        rows.clear();
        campaign_file_.read(1, true, [&](unsigned, SV line) {
            std::array<SV, 2> row;
            if (!fields(line, row)) throw std::runtime_error("Malformed campaigns.csv row");
            rows.emplace_back(row[0], campaigns.size());
            campaigns.emplace_back(row[0]);
            channels.emplace_back(row[1]);
        });
        sources_.build(rows);
    }

    uint32_t category(SV product) const { return products_.lookup(product, unknown_); }
    uint32_t campaign(SV id) const { return sources_.lookup(id, 0); }
};
} // namespace ss
