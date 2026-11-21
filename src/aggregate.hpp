#pragma once

#include "keys.hpp"
#include <algorithm>
#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace ss {

struct Event {
    uint32_t user;
    int64_t ts;
    int32_t price;  // zero for views and carts
    uint32_t category;
};

struct Click {
    uint32_t user, campaign;
    int64_t ts;
    uint64_t id;

    Click() = default;
    Click(uint32_t person, int64_t time, uint32_t source, uint64_t click_id)
        : user(person), campaign(source), ts(time), id(click_id) {}
};

struct UserDay {
    uint32_t user;
    int32_t day;
    int64_t sessions, events, revenue;
    int32_t top;
};

struct CampaignDay {
    uint32_t campaign;
    int32_t day;
    int64_t sessions, purchasing, revenue, assists;
};

namespace agg_detail {
constexpr int64_t gap_ms = 1'800'000;
constexpr int64_t reach_ms = 604'800'000;
constexpr int64_t day_ms = 86'400'000;

inline int32_t day_number(int64_t ms) {
    return static_cast<int32_t>(ms / day_ms - (ms % day_ms < 0));
}

// Most shards have a compact range after removing the low user bits shared
// by every row. Counting-sort those users, then sort their timelines inside
// each contiguous slice. For scattered keys, partition only the high nine
// bits so memory is bounded and sort each smaller partition by both keys.
template <class Row>
inline void sort_timelines(std::vector<Row>& rows) {
    const auto by_time = [](const Row& a, const Row& b) { return a.ts < b.ts; };
    const auto by_user_time = [](const Row& a, const Row& b) {
        return a.user == b.user ? a.ts < b.ts : a.user < b.user;
    };
    if (rows.size() < 1024) {
        std::sort(rows.begin(), rows.end(), by_user_time);
        return;
    }
    uint32_t varying = 0;
    for (const Row& row : rows) varying |= row.user ^ rows.front().user;
    if (!varying) {
        std::sort(rows.begin(), rows.end(), by_time);
        return;
    }
    const uint32_t last_bit = 31 - __builtin_clz(varying);
    uint32_t shift = __builtin_ctz(varying);
    uint32_t mask = (UINT32_MAX >> (31 - last_bit)) >> shift;
    const bool one_user_per_bucket = uint64_t(mask) + 1 <= 2 * rows.size();
    if (!one_user_per_bucket) {
        shift = last_bit > 8 ? last_bit - 8 : 0;
        mask = 511;
    }
    const auto bucket = [&](const Row& row) { return (row.user >> shift) & mask; };
    std::vector<size_t> positions(size_t(mask) + 1);
    for (const Row& row : rows) ++positions[bucket(row)];
    size_t total = 0;
    for (size_t& count : positions) {
        const size_t start = total;
        total += count;
        count = start;
    }
    {
        std::vector<Row> grouped(rows.size());
        for (const Row& row : rows) grouped[positions[bucket(row)]++] = row;
        rows.swap(grouped);
    }
    size_t start = 0;
    for (size_t end : positions) {
        if (end - start > 1) {
            if (one_user_per_bucket) std::sort(rows.begin() + start, rows.begin() + end, by_time);
            else std::sort(rows.begin() + start, rows.begin() + end, by_user_time);
        }
        start = end;
    }
}

struct Session {
    int64_t start;
    int64_t revenue;
    int32_t day;
    size_t day_end;  // exclusive session index after the last start on this day
};

class DailyCategories {
    std::vector<int64_t> sums;
    std::vector<uint32_t> touched;

public:
    void add(uint32_t category, int32_t price) {
        if (category >= sums.size()) sums.resize(size_t(category) + 1);
        if (!sums[category]) touched.push_back(category);
        sums[category] += price;
    }

    int32_t finish_day() {
        int32_t top = -1;
        int64_t maximum = 0;
        for (uint32_t category : touched) {
            if (sums[category] > maximum ||
                (sums[category] == maximum && category < static_cast<uint32_t>(top))) {
                top = static_cast<int32_t>(category);
                maximum = sums[category];
            }
            sums[category] = 0;
        }
        touched.clear();
        return top;
    }
};

struct CampaignLayout {
    int32_t first_day{};
    uint32_t days{}, campaigns{};
};

class CampaignTotals {
    std::unordered_map<uint64_t, size_t> positions;
    std::vector<CampaignDay>& rows;
    CampaignLayout layout;
    std::vector<uint32_t> dense;

public:
    CampaignTotals(std::vector<CampaignDay>& output, CampaignLayout bounds)
        : rows(output), layout(bounds) {
        const uint64_t slots = uint64_t(bounds.days) * bounds.campaigns;
        if (slots && slots <= 262144) dense.assign(slots, UINT32_MAX);
    }

    CampaignDay& get(uint32_t campaign, int32_t day) {
        if (!dense.empty()) {
            auto& index = dense[size_t(day-layout.first_day) * layout.campaigns + campaign];
            if (index == UINT32_MAX) {
                index = static_cast<uint32_t>(rows.size());
                rows.push_back({campaign, day, 0, 0, 0, 0});
            }
            return rows[index];
        }
        uint64_t key = (uint64_t(static_cast<uint32_t>(day)) << 32) | campaign;
        auto [pos, fresh] = positions.try_emplace(key, rows.size());
        if (fresh) rows.push_back({campaign, day, 0, 0, 0, 0});
        return rows[pos->second];
    }
};

// Rows for this person are sorted in increasing time. Close each day only
// after the final session starting that day, including all its later events.
inline void read_sessions(const std::vector<Event>& events, size_t& index,
                          std::vector<Session>& sessions, DailyCategories& categories,
                          std::vector<UserDay>& users) {
    sessions.clear();
    const uint32_t user = events[index].user;
    size_t first_session = 0;
    UserDay daily{user, day_number(events[index].ts), 0, 0, 0, -1};
    auto finish_day = [&] {
        daily.top = categories.finish_day();
        users.push_back(daily);
        while (first_session < sessions.size()) sessions[first_session++].day_end = sessions.size();
    };
    do {
        const size_t start_index = index;
        const int64_t start = events[index].ts;
        const int32_t day = day_number(start);
        if (day != daily.day) {
            finish_day();
            daily = {user, day, 0, 0, 0, -1};
        }
        int64_t revenue = 0;
        do {
            const Event& event = events[index++];
            if (event.price) {
                revenue += event.price;
                categories.add(event.category, event.price);
            }
        } while (index < events.size() && events[index].user == user &&
                 events[index].ts - events[index - 1].ts <= gap_ms);
        ++daily.sessions;
        daily.events += static_cast<int64_t>(index - start_index);
        daily.revenue += revenue;
        sessions.push_back({start, revenue, day, 0});
    } while (index < events.size() && events[index].user == user);
    finish_day();
}

struct Coverage {
    uint32_t user = 0;
    size_t end = 0;
};

inline void attribute(const std::vector<Session>& sessions, uint32_t user,
                      const std::vector<Click>& clicks, size_t& index,
                      std::vector<Coverage>& covered, CampaignTotals& totals, Keys& keys) {
    size_t first = 0, end = 0;
    const Click* latest = nullptr;
    auto credit = [&](const Session& s) {
        const uint32_t campaign = latest && s.start - latest->ts <= reach_ms ? latest->campaign : 0;
        auto& row = totals.get(campaign, s.day);
        ++row.sessions;
        row.purchasing += s.revenue > 0;
        row.revenue += s.revenue;
    };
    while (index < clicks.size() && clicks[index].user == user) {
        const Click& click = clicks[index++];
        while (first < sessions.size() && sessions[first].start < click.ts) credit(sessions[first++]);

        // Each click covers a contiguous interval of session starts. The two
        // endpoints only move forward. Count the previously uncovered suffix
        // for this campaign, in one addition per day spanned by that suffix.
        end = std::max(end, first);
        while (end < sessions.size() && sessions[end].start - click.ts <= reach_ms) ++end;
        if (first < end) {
            if (click.campaign >= covered.size()) covered.resize(size_t(click.campaign) + 1);
            Coverage& prior = covered[click.campaign];
            size_t index_in_day = prior.user == user ? std::max(first, prior.end) : first;
            for (size_t next; index_in_day < end; index_in_day = next) {
                const Session& s = sessions[index_in_day];
                next = std::min(end, s.day_end);
                totals.get(click.campaign, s.day).assists += static_cast<int64_t>(next - index_in_day);
            }
            prior = {user, end};
        }
        // Equal-time clicks are unordered, so apply the bytewise ID tie rule
        // before crediting any start at this timestamp.
        if (!latest || latest->ts < click.ts || keys.less(click.id, latest->id)) latest = &click;
    }
    while (first < sessions.size()) credit(sessions[first++]);
}
}  // namespace agg_detail

inline void aggregate(std::vector<Event>& events, std::vector<Click>& clicks,
                      std::vector<UserDay>& users, std::vector<CampaignDay>& campaigns,
                      agg_detail::CampaignLayout layout, Keys& keys) {
    if (events.empty()) return;
    agg_detail::sort_timelines(events);
    agg_detail::sort_timelines(clicks);

    agg_detail::DailyCategories categories;
    agg_detail::CampaignTotals totals(campaigns, layout);
    std::vector<agg_detail::Session> sessions;
    std::vector<agg_detail::Coverage> covered;
    size_t event_index = 0, click_index = 0;
    while (event_index < events.size()) {
        const uint32_t user = events[event_index].user;
        agg_detail::read_sessions(events, event_index, sessions, categories, users);
        while (click_index < clicks.size() && clicks[click_index].user < user) ++click_index;
        agg_detail::attribute(sessions, user, clicks, click_index, covered, totals, keys);
    }
}
}  // namespace ss
