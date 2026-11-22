#pragma once

#include <atomic>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <vector>

namespace blitz {

// Safe runtime THP probe: enables MADV_HUGEPAGE only when the kernel either never
// triggers synchronous compaction (`[never]`/`[defer]`/`[defer+madvise]`) OR already
// has >= 4 GiB of contiguous 2 MiB blocks free in `/proc/buddyinfo` Normal zone.
inline bool safe_for_thp() {
    static const bool ok = []() -> bool {
        std::ifstream df("/sys/kernel/mm/transparent_hugepage/defrag");
        std::string dline;
        if (df && std::getline(df, dline)) {
            if (dline.find("[never]") != std::string::npos ||
                dline.find("[defer]") != std::string::npos ||
                dline.find("[defer+madvise]") != std::string::npos) {
                return true;
            }
        }
        std::ifstream bi("/proc/buddyinfo");
        std::string line;
        uint64_t ready_2mb = 0;
        while (bi && std::getline(bi, line)) {
            size_t pos = line.find("Normal");
            if (pos == std::string::npos) continue;
            std::istringstream iss(line.substr(pos + 6));
            uint64_t ord[11]{};
            int k = 0;
            while (k < 11 && (iss >> ord[k])) ++k;
            if (k >= 10) ready_2mb += ord[9] + (k >= 11 ? 2 * ord[10] : 0);
        }
        return ready_2mb >= 2048;
    }();
    return ok;
}

// Per-worker private 16 KiB slab allocator backed by 64 MiB anonymous mmap chunks.
// Zero cross-thread VMA lock contention, zero atomic contention, and supports
// O(1) reset() to reuse already-faulted physical RAM across pipeline stages!
class WorkerArena {
public:
    static constexpr size_t SLAB_SHIFT = 14;         // 16 KiB per slab (1024 x 16B items)
    static constexpr size_t SLAB_BYTES = size_t{1} << SLAB_SHIFT;
    static constexpr size_t CHUNK_SLABS_SHIFT = 12;  // 4096 slabs = 64 MiB per chunk
    static constexpr size_t CHUNK_SLABS = size_t{1} << CHUNK_SLABS_SHIFT;
    static constexpr size_t CHUNK_BYTES = CHUNK_SLABS * SLAB_BYTES;
    static constexpr size_t MAX_CHUNKS = 64;         // 4 GiB per worker (32 GiB across 8 workers)

private:
    uint8_t* chunks_[MAX_CHUNKS]{};
    uint32_t next_slab_ = 0;

    uint8_t* alloc_chunk(uint32_t cid) {
        void* mem = ::mmap(nullptr, CHUNK_BYTES, PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (mem == MAP_FAILED) throw std::runtime_error("WorkerArena mmap failed");
#ifdef MADV_HUGEPAGE
        if (safe_for_thp()) ::madvise(mem, CHUNK_BYTES, MADV_HUGEPAGE);
#endif
        chunks_[cid] = static_cast<uint8_t*>(mem);
        return chunks_[cid];
    }

public:
    WorkerArena() = default;

    inline uint32_t alloc_slab(void** out_ptr) {
        uint32_t id = next_slab_++;
        uint32_t cid = id >> CHUNK_SLABS_SHIFT;
        if (__builtin_expect(cid >= MAX_CHUNKS, 0)) throw std::runtime_error("WorkerArena exhausted");
        uint8_t* base = chunks_[cid];
        if (__builtin_expect(!base, 0)) base = alloc_chunk(cid);
        *out_ptr = base + (size_t(id & (CHUNK_SLABS - 1)) << SLAB_SHIFT);
        return id;
    }

    inline uint8_t* slab_ptr(uint32_t slab_id) const {
        return chunks_[slab_id >> CHUNK_SLABS_SHIFT] +
               (size_t(slab_id & (CHUNK_SLABS - 1)) << SLAB_SHIFT);
    }

    // Reuse all already-faulted physical pages in the next stage (zero page faults!).
    inline void reset() { next_slab_ = 0; }
};

template <class T>
struct SlabRef {
    T* ptr;
    uint32_t count;
};

template <class T>
class SlabList {
public:
    static constexpr uint32_t CAP = uint32_t(WorkerArena::SLAB_BYTES / sizeof(T));

private:
    T* cur_ = nullptr;
    uint32_t cur_slab_id_ = 0;
    uint32_t cur_count_ = CAP;
    std::vector<SlabRef<T>> full_;

public:
    inline uint32_t push_with_loc(WorkerArena& arena, uint32_t worker_prefix, const T& val) {
        if (__builtin_expect(cur_count_ == CAP, 0)) {
            if (cur_) full_.push_back({cur_, cur_count_});
            void* p = nullptr;
            cur_slab_id_ = arena.alloc_slab(&p);
            cur_ = static_cast<T*>(p);
            cur_count_ = 0;
        }
        uint32_t idx = cur_count_++;
        cur_[idx] = val;
        return worker_prefix | (cur_slab_id_ << 10) | idx;
    }

    inline void push(WorkerArena& arena, const T& val) {
        if (__builtin_expect(cur_count_ == CAP, 0)) {
            if (cur_) full_.push_back({cur_, cur_count_});
            void* p = nullptr;
            cur_slab_id_ = arena.alloc_slab(&p);
            cur_ = static_cast<T*>(p);
            cur_count_ = 0;
        }
        cur_[cur_count_++] = val;
    }

    void finalize() {
        if (cur_ && cur_count_ > 0) {
            full_.push_back({cur_, cur_count_});
            cur_ = nullptr;
            cur_count_ = CAP;
        }
    }

    const std::vector<SlabRef<T>>& slabs() const { return full_; }

    size_t total_count() const {
        size_t n = 0;
        for (const auto& s : full_) n += s.count;
        return n;
    }
};

} // namespace blitz
