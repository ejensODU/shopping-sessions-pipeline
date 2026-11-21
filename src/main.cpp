#include "catalog.hpp"
#include "output.hpp"
#include <array>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <limits>
#ifdef __GLIBC__
#include <malloc.h>
#endif
#include <regex>

namespace ss {
constexpr size_t partitions = 128;
constexpr size_t people_shards = 64;
constexpr int64_t day_ms = 86'400'000;
constexpr auto bad_time = std::numeric_limits<int64_t>::min();

struct Window {
    int64_t start, end;
    explicit Window(const fs::path& dir) {
        std::ifstream file(dir / "manifest.json");
        std::string json{std::istreambuf_iterator<char>(file), {}};
        auto value = [&](const char* field) {
            std::smatch found;
            std::regex pattern(std::string("\"") + field + R"re("\s*:\s*"([^"]+)")re");
            if (!std::regex_search(json, found, pattern))
                throw std::runtime_error(std::string("Missing manifest field ") + field);
            const auto t = timestamp(found[1].str());
            if (t == bad_time) throw std::runtime_error("Invalid window timestamp");
            return t;
        };
        start = value("window_start");
        end = value("window_end");
        if (end <= start) throw std::runtime_error("Empty event window");
    }
};

// Every structurally valid event with an ID contributes to the earliest-ingest
// comparison. An invalid payload has a zero user and can still defeat a later copy.
struct PersonEvent {
    uint64_t user;
    int64_t ts;
    int32_t price;
    uint32_t category;
};

struct RawEvent {
    uint64_t id, user;
    int64_t ingest, ts;
    int32_t price;
    uint32_t category;

    static RawEvent pack(uint64_t id, int64_t ingest, PersonEvent e, const Window&) {
        return {id, e.user, ingest, e.ts, e.price, e.category};
    }
    PersonEvent unpack(const Window&) const { return {user, ts, price, category}; }
};

// If the entire window fits in 32-bit milliseconds and the category rank fits
// in eight bits, store both without changing their ordering or precision.
struct CompactEvent {
    uint64_t id, user;
    int64_t ingest;
    uint32_t offset, purchase;
    static CompactEvent pack(uint64_t id, int64_t ingest, PersonEvent e, const Window& win) {
        const uint32_t offset = e.user ? static_cast<uint32_t>(e.ts - win.start) : 0;
        return {id, e.user, ingest, offset, (e.category << 24) | static_cast<uint32_t>(e.price)};
    }
    PersonEvent unpack(const Window& win) const {
        return {user, win.start + offset, static_cast<int32_t>(purchase & 0xffffff), purchase >> 24};
    }
};
struct PersonClick {
    uint64_t user;
    int64_t ts;
    uint64_t id;
    uint32_t campaign;
};
template<class T, size_t Count> using Buckets = std::array<std::vector<T>, Count>;
template<class Row> using Raw = Buckets<Row, partitions>;
using Events = Buckets<PersonEvent, people_shards>;
using Clicks = Buckets<PersonClick, people_shards>;

template<class Row>
void parse_events(const fs::path& data, Keys& keys, const Window& window,
                  const Catalog& catalog, std::vector<Raw<Row>>& raw) {
    Input input(data / "events");
    // Capacity is a hint. No correctness decision uses the shape of a dataset.
    const size_t estimate = input.bytes / (80 * raw.size() * partitions);
    for (auto& worker : raw) for (auto& bucket : worker) bucket.reserve(estimate);
    input.read(raw.size(), false, [&](unsigned w, SV line) {
        std::array<SV,7> f;
        if (!fields(line, f) || f[0].empty()) return;
        const uint64_t id = keys.encode(f[0]);
        const int64_t ingest = ingest_time(f[3]);
        const int64_t ts = timestamp(f[2]);
        PersonEvent event{0, ts, 0, 0};
        if (ts >= window.start && ts < window.end && ingest - ts <= day_ms &&
            !f[1].empty() && !f[5].empty()) {
            if (f[4] == "view" || f[4] == "cart") event.user = keys.encode(f[1]);
            else if (f[4] == "purchase") {
                event.price = price(f[6]);
                if (event.price) {
                    event.category = catalog.category(f[5]);
                    event.user = keys.encode(f[1]);
                }
            }
        }
        raw[w][mix(id) & (partitions-1)].push_back(Row::pack(id, ingest, event, window));
    });
}

struct CachedPerson {
    uint64_t key = 0, canonical = 0;
    size_t shard = 0;
};
class Resolver {
    std::vector<CachedPerson> cache_{size_t{1} << 15};
public:
    const CachedPerson& get(uint64_t user, const People& people) {
        auto& entry = cache_[mix(user) & (cache_.size() - 1)];
        if (entry.key != user) {
            const auto canonical = people.resolve(user);
            entry = {user, canonical, mix(canonical) & (people_shards - 1)};
        }
        return entry;
    }
};

template<class Row>
void deduplicate(std::vector<Raw<Row>>& raw, const Window& window, const People& people,
                 std::vector<Events>& events, std::vector<Resolver>& resolvers) {
    size_t total = 0;
    for (const auto& worker : raw) for (const auto& bucket : worker) total += bucket.size();
    for (auto& worker : events) for (auto& bucket : worker)
        bucket.reserve(total / (events.size() * people_shards));
    parallel(partitions, raw.size(), [&](unsigned w, size_t b) {
        size_t count = 0, capacity = 8;
        for (const auto& worker : raw) count += worker[b].size();
        while (capacity - capacity/4 < count) capacity *= 2;
        std::vector<Row*> table(capacity, nullptr);
        for (auto& worker : raw) for (auto& row : worker[b]) {
            // Low hash bits already chose b. Use the remaining bits inside b.
            size_t i = (mix(row.id) / partitions) & (capacity-1);
            while (table[i] && table[i]->id != row.id) i = (i+1) & (capacity-1);
            auto& winner = table[i];
            if (winner && winner->ingest <= row.ingest) { row.user = 0; continue; }
            if (winner) winner->user = 0;
            winner = &row;
        }
        for (auto& worker : raw) {
            for (const auto& row : worker[b]) if (row.user) {
                const auto& p = resolvers[w].get(row.user, people);
                auto event = row.unpack(window);
                event.user = p.canonical;
                events[w][p.shard].push_back(event);
            }
            std::vector<Row>().swap(worker[b]);
        }
    });
}

void parse_clicks(Input& input, Keys& keys, const People& people,
                  const Window& window, const Catalog& catalog,
                  std::vector<Clicks>& clicks, std::vector<Resolver>& resolvers) {
    const size_t estimate = input.bytes / (48 * clicks.size() * people_shards);
    for (auto& worker : clicks) for (auto& bucket : worker) bucket.reserve(estimate);
    input.read(clicks.size(), false, [&](unsigned w, SV line) {
        std::array<SV,4> f;
        if (!fields(line, f) || f[0].empty() || f[1].empty()) return;
        const auto campaign = catalog.campaign(f[2]);
        if (!campaign) return;
        const int64_t ts = timestamp(f[3]);
        if (ts < window.start - 7*day_ms || ts >= window.end) return;
        const auto& p = resolvers[w].get(keys.encode(f[1]), people);
        // Equal copies have the same ID, campaign and time. They neither change
        // the winner nor extend the union of the campaign's assist intervals.
        clicks[w][p.shard].push_back({p.canonical, ts, keys.encode(f[0]), campaign});
    });
}

template<class Worker>
size_t count_bucket(const std::vector<Worker>& workers, size_t bucket) {
    size_t total = 0;
    for (const auto& worker : workers) total += worker[bucket].size();
    return total;
}

bool finish_shard(size_t b, std::vector<Events>& incoming, std::vector<Clicks>& incoming_clicks,
                  std::vector<CampaignDay>& totals, Keys& keys, const Catalog& catalog,
                  const fs::path& user_dir, agg_detail::CampaignLayout layout) {
    const size_t size = count_bucket(incoming, b);
    std::vector<Event> events;
    std::vector<Click> clicks;
    std::vector<std::string> names;
    KeyMap<uint32_t> local(size / 32);
    auto user = [&](uint64_t key) {
        auto [id, inserted] = local.insert(key, static_cast<uint32_t>(names.size()));
        if (inserted) names.push_back(keys.decode(key));
        return *id;
    };
    events.reserve(size);
    for (auto& worker : incoming) {
        for (auto& row : worker[b]) events.push_back({user(row.user), row.ts, row.price, row.category});
        std::vector<PersonEvent>().swap(worker[b]);
    }
    clicks.reserve(count_bucket(incoming_clicks, b));
    for (auto& worker : incoming_clicks) {
        for (auto& row : worker[b]) {
            if (const auto* id = local.find(row.user))
                clicks.emplace_back(*id, row.ts, row.campaign, row.id);
        }
        std::vector<PersonClick>().swap(worker[b]);
    }
    if (events.empty()) return false;
    std::vector<UserDay> users;
    aggregate(events, clicks, users, totals, layout, keys);
    write_users(user_dir / ("part-" + std::to_string(b) + ".parquet"), users, names, catalog.categories);
    return true;
}

void run(const fs::path& data, const fs::path& out, unsigned threads) {
    auto last = std::chrono::steady_clock::now();
    auto mark = [&](const char* name) {
        auto now = std::chrono::steady_clock::now();
        if (std::getenv("PROFILE")) std::cerr << name << " " << std::chrono::duration<double>(now-last).count() << " s\n";
        last = now;
    };
    Window window(data);
    Keys keys;
    Catalog catalog(data);
    People people(data, keys, threads);
    mark("catalogs and links");

    std::vector<Events> events(threads);
    std::vector<Resolver> resolvers(threads);
    auto process_events = [&]<class Row>() {
        std::vector<Raw<Row>> raw(threads);
        parse_events(data, keys, window, catalog, raw);
        mark("event parsing");
        deduplicate(raw, window, people, events, resolvers);
        mark("event deduplication and identity resolution");
    };
    if (window.end - window.start <= UINT32_MAX && catalog.categories.size() <= 256)
        process_events.template operator()<CompactEvent>();
    else
        process_events.template operator()<RawEvent>();

    Input click_input(data / "clicks");
    std::vector<Clicks> clicks(threads);
    parse_clicks(click_input, keys, people, window, catalog, clicks, resolvers);
    mark("click parsing and identity resolution");

    fs::create_directories(out / "user_daily");
    fs::create_directories(out / "campaign_daily");
    std::array<std::vector<CampaignDay>, people_shards> partial;
    const int32_t first = agg_detail::day_number(window.start);
    const uint32_t days = static_cast<uint32_t>(agg_detail::day_number(window.end-1) - first + 1);
    agg_detail::CampaignLayout layout{first, days, static_cast<uint32_t>(catalog.campaigns.size())};
    std::atomic<bool> have_users{false};
    parallel(people_shards, threads, [&](unsigned, size_t b) {
        if (finish_shard(b, events, clicks, partial[b], keys, catalog, out / "user_daily", layout))
            have_users.store(true, std::memory_order_relaxed);
    });
    if (!have_users.load()) write_users(out / "user_daily/part-0.parquet", {}, {}, catalog.categories);
    mark("sessions, attribution and user output");

    std::vector<CampaignDay> daily;
    std::unordered_map<uint64_t, size_t> positions;
    for (auto& shard : partial) for (const auto& row : shard) {
        uint64_t id = uint64_t(static_cast<uint32_t>(row.day)) << 32 | row.campaign;
        auto [i, fresh] = positions.try_emplace(id, daily.size());
        if (fresh) { daily.push_back(row); continue; }
        auto& total = daily[i->second];
        total.sessions += row.sessions;
        total.purchasing += row.purchasing;
        total.revenue += row.revenue;
        total.assists += row.assists;
    }
    write_campaigns(out / "campaign_daily/part-0.parquet", daily, catalog.campaigns, catalog.channels);
    mark("campaign output");
}
} // namespace ss

int main(int argc, char** argv) {
    try {
        auto path = [&](int i, const char* env, const char* fallback) {
            if (argc > i) return std::string(argv[i]);
            const char* value = std::getenv(env);
            return std::string(value ? value : fallback);
        };
        const char* cpu = std::getenv("NCPU");
        unsigned threads = cpu ? std::clamp(std::atoi(cpu), 1, 8) : 8;
#ifdef __GLIBC__
        // Return large, completed partitions to the OS during the next phase.
        // glibc's adaptive threshold can otherwise retain multi-MiB buffers.
        mallopt(M_MMAP_THRESHOLD, 256 * 1024);
#endif
        ss::run(path(1, "DATA_DIR", "/data"), path(2, "OUTPUT_DIR", "/output"), threads);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Pipeline failed: " << e.what() << '\n';
        return 1;
    }
}
