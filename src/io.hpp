#pragma once

#include "parse.hpp"
#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <fcntl.h>
#include <filesystem>
#include <functional>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

namespace blitz {

namespace fs = std::filesystem;

// Persistent low-latency worker pool (eliminates ~50 pthread_create/join barriers across stages).
class ThreadPool {
    std::mutex mu_;
    std::condition_variable cv_work_;
    std::condition_variable cv_done_;
    std::vector<std::thread> threads_;
    std::function<void(unsigned)> task_;
    uint64_t epoch_ = 0;
    unsigned active_n_ = 0;
    unsigned remaining_ = 0;
    bool stop_ = false;

    void worker_loop(unsigned wid) {
        uint64_t seen = 0;
        for (;;) {
            std::function<void(unsigned)> fn;
            bool run_me = false;
            {
                std::unique_lock lk(mu_);
                cv_work_.wait(lk, [&]() { return epoch_ != seen || stop_; });
                if (stop_) return;
                seen = epoch_;
                if (wid < active_n_) {
                    fn = task_;
                    run_me = true;
                }
            }
            if (run_me) {
                fn(wid);
                std::lock_guard lk(mu_);
                if (--remaining_ == 0) cv_done_.notify_one();
            }
        }
    }

public:
    ThreadPool() = default;

    ~ThreadPool() {
        {
            std::lock_guard lk(mu_);
            stop_ = true;
            ++epoch_;
        }
        cv_work_.notify_all();
        for (auto& t : threads_) t.join();
    }

    void ensure(unsigned n) {
        if (n <= 1) return;
        std::lock_guard lk(mu_);
        while (threads_.size() + 1 < n) {
            unsigned wid = unsigned(threads_.size() + 1);
            threads_.emplace_back(&ThreadPool::worker_loop, this, wid);
        }
    }

    template <class Fn>
    void run(unsigned n, Fn&& fn) {
        if (n <= 1) {
            fn(0u);
            return;
        }
        ensure(n);
        {
            std::lock_guard lk(mu_);
            task_ = std::ref(fn);
            active_n_ = n;
            remaining_ = n - 1;
            ++epoch_;
        }
        cv_work_.notify_all();
        fn(0u);
        std::unique_lock lk(mu_);
        cv_done_.wait(lk, [&]() { return remaining_ == 0; });
        task_ = nullptr;
    }
};

inline ThreadPool& global_pool() {
    static ThreadPool pool;
    return pool;
}

template <class Fn>
inline void parallel_for(size_t count, unsigned threads, Fn&& fn) {
    if (count == 0) return;
    const unsigned n = std::min<unsigned>(threads, unsigned(count));
    if (n <= 1) {
        for (size_t i = 0; i < count; ++i) fn(0u, i);
        return;
    }
    std::atomic<size_t> next{0};
    std::mutex err_mu;
    std::exception_ptr err;
    auto worker = [&](unsigned wid) {
        try {
            for (;;) {
                size_t i = next.fetch_add(1, std::memory_order_relaxed);
                if (i >= count) break;
                fn(wid, i);
            }
        } catch (...) {
            next.store(count, std::memory_order_relaxed);
            std::lock_guard lk(err_mu);
            if (!err) err = std::current_exception();
        }
    };
    global_pool().run(n, worker);
    if (err) std::rethrow_exception(err);
}

inline size_t pread_full(int fd, char* dst, size_t len, size_t off) {
    size_t done = 0;
    while (done < len) {
        ssize_t r = ::pread(fd, dst + done, len - done, off_t(off + done));
        if (r <= 0) break;
        done += size_t(r);
    }
    return done;
}

inline size_t find_header_end(int fd, size_t file_size) {
    char tmp[4096];
    size_t off = 0;
    while (off < file_size) {
        size_t n = pread_full(fd, tmp, std::min(sizeof(tmp), file_size - off), off);
        if (n == 0) return file_size;
        const char* nl = static_cast<const char*>(std::memchr(tmp, '\n', n));
        if (nl) return off + size_t(nl - tmp) + 1;
        off += n;
    }
    return file_size;
}

// High-throughput pread()-based parallel CSV reader.
// Eliminates mmap minor-fault contention and munmap/exit_group PTE zap storms
// while feeding 64-byte aligned L2-resident 256 KiB buffers directly into the
// single-pass AVX2 / AVX-512 CSV block scanner.
class PreadInput {
    struct FileEntry {
        int fd = -1;
        size_t size = 0;
        size_t header_end = 0;
    };
    struct Task {
        int fd;
        size_t file_size;
        size_t start; // inclusive byte offset of task ownership
        size_t end;   // exclusive byte offset of task ownership
        bool aligned; // true if `start` is guaranteed to begin a line (header_end)
    };

    std::vector<FileEntry> files_;
    std::vector<Task> tasks_;

    void add_file(const fs::path& path, size_t task_bytes) {
        int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) throw std::runtime_error("Could not open " + path.string());
        size_t sz = fs::file_size(path);
        if (sz == 0) {
            ::close(fd);
            return;
        }
        size_t hdr_end = find_header_end(fd, sz);
        files_.push_back({fd, sz, hdr_end});
        total_bytes += sz;
        if (hdr_end >= sz) return; // header-only file
        if (sz - hdr_end <= task_bytes) {
            tasks_.push_back({fd, sz, hdr_end, sz, true});
        } else {
            size_t pos = hdr_end;
            bool first = true;
            while (pos < sz) {
                size_t stop = std::min(sz, pos + task_bytes);
                tasks_.push_back({fd, sz, pos, stop, first});
                first = false;
                pos = stop;
            }
        }
    }

public:
    size_t total_bytes = 0;

    explicit PreadInput(const fs::path& path, size_t task_bytes = 2u << 20) {
        if (!fs::is_directory(path)) {
            add_file(path, task_bytes);
            return;
        }
        std::vector<std::pair<size_t, fs::path>> paths;
        for (const auto& entry : fs::directory_iterator(path)) {
            if (entry.is_regular_file() && entry.path().extension() == ".csv") {
                paths.emplace_back(entry.file_size(), entry.path());
            }
        }
        std::sort(paths.begin(), paths.end(),
                  [](const auto& a, const auto& b) { return a.first > b.first; });
        for (const auto& [sz, p] : paths) add_file(p, task_bytes);
    }

    ~PreadInput() {
        for (auto& f : files_) {
            if (f.fd >= 0) ::close(f.fd);
        }
    }

    PreadInput(const PreadInput&) = delete;
    PreadInput& operator=(const PreadInput&) = delete;

    // Stream raw 64-byte-aligned L2-hot buffers `[buf, buf + scan_len)` to `on_buf(wid, buf, scan_len)`.
    // GUARANTEE: `buf[scan_len - 1] == '\n'` (even if the file lacked a trailing newline at EOF),
    // and there are at least 128 readable zero bytes at `buf + scan_len`.
    // Every data line in every file is delivered to exactly one `on_buf` call.
    template <class OnBuf, class OnFlush>
    void scan_buffers(unsigned threads, OnBuf&& on_buf, OnFlush&& on_flush) const {
        if (tasks_.empty()) return;
        const unsigned nworkers = std::min<unsigned>(threads, unsigned(tasks_.size()));
        std::atomic<size_t> next{0};
        std::mutex err_mu;
        std::exception_ptr err;

        auto worker = [&](unsigned wid) {
            try {
                constexpr size_t READ_CHUNK = 512u << 10; // 512 KiB: halves pread syscalls, fits L2/L3!
                size_t buf_cap = READ_CHUNK + 4096;
                char* buf = static_cast<char*>(std::aligned_alloc(64, buf_cap + 256));
                if (!buf) throw std::bad_alloc();

                auto ensure_cap = [&](size_t needed) {
                    if (needed <= buf_cap) return;
                    size_t new_cap = buf_cap;
                    while (new_cap < needed) new_cap *= 2;
                    char* nbuf = static_cast<char*>(std::aligned_alloc(64, new_cap + 256));
                    if (!nbuf) throw std::bad_alloc();
                    std::memcpy(nbuf, buf, buf_cap);
                    std::free(buf);
                    buf = nbuf;
                    buf_cap = new_cap;
                };

                for (;;) {
                    size_t ti = next.fetch_add(1, std::memory_order_relaxed);
                    if (ti >= tasks_.size()) break;
                    const Task& tk = tasks_[ti];

                    size_t pos = tk.start;
                    if (!tk.aligned) {
                        size_t probe_off = tk.start - 1;
                        bool found = false;
                        char probe[4096];
                        size_t probe_step = 256;
                        while (probe_off < tk.file_size) {
                            size_t n = pread_full(tk.fd, probe,
                                                  std::min(probe_step, tk.file_size - probe_off),
                                                  probe_off);
                            if (n == 0) break;
                            const char* nl = static_cast<const char*>(std::memchr(probe, '\n', n));
                            if (nl) {
                                pos = probe_off + size_t(nl - probe) + 1;
                                found = true;
                                break;
                            }
                            probe_off += n;
                            probe_step = sizeof(probe);
                        }
                        if (!found || pos >= tk.end) continue;
                    }

                    size_t carry = 0;
                    size_t buf_file_off = pos;
                    size_t read_off = pos;
                    while (buf_file_off < tk.end) {
                        size_t want = READ_CHUNK;
                        if (read_off < tk.end) {
                            size_t rem_to_end = tk.end - read_off;
                            if (rem_to_end < READ_CHUNK) {
                                want = std::min(tk.file_size - read_off, rem_to_end + 512);
                            }
                        } else {
                            want = std::min(tk.file_size - read_off, size_t{4096});
                        }
                        if (want == 0) break;
                        ensure_cap(carry + want);
                        size_t got = pread_full(tk.fd, buf + carry, want, read_off);
                        if (got == 0) break;
                        read_off += got;
                        size_t valid_len = carry + got;

                        if (buf_file_off + valid_len < tk.end) {
                            // Find last '\n' in [0, valid_len) so every line in [0, scan_end) ends in '\n'.
                            const char* last_nl = static_cast<const char*>(
                                ::memrchr(buf, '\n', valid_len));
                            if (last_nl) {
                                size_t scan_end = size_t(last_nl - buf) + 1;
                                char saved[64];
                                size_t tail_len = valid_len - scan_end;
                                // Note: preserve tail_len bytes past scan_end, only pad after valid_len!
                                // Better: call on_buf(wid, buf, scan_end) while buffer has 128B pad at valid_len,
                                // and since tail_len >= 0, bytes [scan_end..scan_end+64] are readable!
                                // Zero-pad past valid_len so even a 64B read at end of buffer is safe.
                                std::memset(buf + valid_len, 0, 128);
                                (void)saved;
                                on_buf(wid, buf, scan_end);
                                carry = tail_len;
                                if (carry > 0) std::memmove(buf, buf + scan_end, carry);
                                buf_file_off += scan_end;
                            } else {
                                carry = valid_len;
                            }
                        } else {
                            size_t limit = tk.end - buf_file_off;
                            if (buf[limit - 1] == '\n') {
                                std::memset(buf + limit, 0, 128);
                                on_buf(wid, buf, limit);
                                break;
                            }
                            const char* nl = static_cast<const char*>(
                                std::memchr(buf + limit, '\n', valid_len - limit));
                            if (nl) {
                                size_t scan_end = size_t(nl - buf) + 1;
                                std::memset(buf + scan_end, 0, 128);
                                on_buf(wid, buf, scan_end);
                                break;
                            }
                            if (read_off >= tk.file_size) {
                                // EOF reached; append synthetic '\n' if last byte wasn't '\n'.
                                size_t scan_end = valid_len;
                                if (scan_end > 0 && buf[scan_end - 1] != '\n') {
                                    buf[scan_end++] = '\n';
                                }
                                if (scan_end > 0) {
                                    std::memset(buf + scan_end, 0, 128);
                                    on_buf(wid, buf, scan_end);
                                }
                                break;
                            }
                            // Rare: line extends >512 bytes past tk.end. Consume complete lines before limit.
                            const char* last_nl = static_cast<const char*>(
                                ::memrchr(buf, '\n', limit));
                            if (last_nl) {
                                size_t scan_end = size_t(last_nl - buf) + 1;
                                std::memset(buf + valid_len, 0, 128);
                                on_buf(wid, buf, scan_end);
                                carry = valid_len - scan_end;
                                if (carry > 0) std::memmove(buf, buf + scan_end, carry);
                                buf_file_off += scan_end;
                            } else {
                                carry = valid_len;
                            }
                        }
                    }
                }
                on_flush(wid);
                std::free(buf);
            } catch (...) {
                next.store(tasks_.size(), std::memory_order_relaxed);
                std::lock_guard lk(err_mu);
                if (!err) err = std::current_exception();
            }
        };

        global_pool().run(nworkers, worker);
        if (err) std::rethrow_exception(err);
    }

    // Run `on_row(unsigned worker_id, const std::array<SV, N>& f)` over every
    // structurally valid N-field CSV line. `on_flush(unsigned worker_id)` is
    // called once per worker after finishing all tasks.
    template <size_t N, class OnRow, class OnFlush>
    void scan(unsigned threads, OnRow&& on_row, OnFlush&& on_flush) const {
        if (tasks_.empty()) return;
        const unsigned nworkers = std::min<unsigned>(threads, unsigned(tasks_.size()));
        std::atomic<size_t> next{0};
        std::mutex err_mu;
        std::exception_ptr err;

        auto worker = [&](unsigned wid) {
            try {
                constexpr size_t READ_CHUNK = 256u << 10; // 256 KiB fits cleanly in L2!
                size_t buf_cap = READ_CHUNK + 4096;
                char* buf = static_cast<char*>(std::aligned_alloc(64, buf_cap + 256));
                if (!buf) throw std::bad_alloc();

                auto ensure_cap = [&](size_t needed) {
                    if (needed <= buf_cap) return;
                    size_t new_cap = buf_cap;
                    while (new_cap < needed) new_cap *= 2;
                    char* nbuf = static_cast<char*>(std::aligned_alloc(64, new_cap + 256));
                    if (!nbuf) throw std::bad_alloc();
                    std::memcpy(nbuf, buf, buf_cap);
                    std::free(buf);
                    buf = nbuf;
                    buf_cap = new_cap;
                };

                auto row_cb = [&](const std::array<SV, N>& f) __attribute__((always_inline)) {
                    on_row(wid, f);
                };

                std::array<SV, N> tail_f;
                for (;;) {
                    size_t ti = next.fetch_add(1, std::memory_order_relaxed);
                    if (ti >= tasks_.size()) break;
                    const Task& tk = tasks_[ti];

                    size_t pos = tk.start;
                    if (!tk.aligned) {
                        // Find first '\n' at offset >= tk.start - 1; first owned line starts right after.
                        size_t probe_off = tk.start - 1;
                        bool found = false;
                        char probe[4096];
                        size_t probe_step = 256;
                        while (probe_off < tk.file_size) {
                            size_t n = pread_full(tk.fd, probe,
                                                  std::min(probe_step, tk.file_size - probe_off),
                                                  probe_off);
                            if (n == 0) break;
                            const char* nl = static_cast<const char*>(std::memchr(probe, '\n', n));
                            if (nl) {
                                pos = probe_off + size_t(nl - probe) + 1;
                                found = true;
                                break;
                            }
                            probe_off += n;
                            probe_step = sizeof(probe);
                        }
                        if (!found || pos >= tk.end) continue;
                    }

                    // Stream lines whose start offset is in [pos, tk.end).
                    size_t carry = 0;
                    size_t buf_file_off = pos; // file offset of buf[0]
                    size_t read_off = pos;     // next file offset to pread
                    while (buf_file_off < tk.end) {
                        size_t want = READ_CHUNK;
                        // Avoid reading wildly past tk.end unless finishing a spanning line.
                        if (read_off < tk.end) {
                            size_t rem_to_end = tk.end - read_off;
                            if (rem_to_end < READ_CHUNK) {
                                // Read up to tk.end plus 512 bytes to capture the final spanning line in 1 syscall!
                                want = std::min(tk.file_size - read_off, rem_to_end + 512);
                            }
                        } else {
                            want = std::min(tk.file_size - read_off, size_t{4096});
                        }
                        if (want == 0) break;
                        ensure_cap(carry + want);
                        size_t got = pread_full(tk.fd, buf + carry, want, read_off);
                        if (got == 0) break;
                        read_off += got;
                        size_t valid_len = carry + got;

                        if (buf_file_off + valid_len < tk.end) {
                            // Entire buffer is strictly before tk.end.
                            std::memset(buf + valid_len, 0, 64);
                            size_t consumed = scan_csv_buffer<N>(buf, valid_len, row_cb);
                            carry = valid_len - consumed;
                            if (carry > 0) std::memmove(buf, buf + consumed, carry);
                            buf_file_off += consumed;
                        } else {
                            // Buffer reaches or passes tk.end.
                            // Scan everything up to and including the first '\n' at or after (tk.end - 1).
                            size_t limit = tk.end - buf_file_off; // >= 1, <= valid_len
                            if (buf[limit - 1] == '\n') {
                                // Last owned line ends cleanly at limit - 1!
                                std::memset(buf + limit, 0, 64);
                                scan_csv_buffer<N>(buf, limit, row_cb);
                                carry = 0;
                                buf_file_off = tk.end;
                                break;
                            }
                            // A line started strictly before tk.end and crosses tk.end; locate its '\n'.
                            const char* nl = static_cast<const char*>(
                                std::memchr(buf + limit, '\n', valid_len - limit));
                            if (nl) {
                                size_t scan_end = size_t(nl - buf) + 1;
                                std::memset(buf + scan_end, 0, 64);
                                scan_csv_buffer<N>(buf, scan_end, row_cb);
                                carry = 0;
                                buf_file_off += scan_end;
                                break;
                            }
                            // Either EOF (no trailing newline) or an extra-long line spanning past +512B.
                            if (read_off >= tk.file_size) {
                                std::memset(buf + valid_len, 0, 64);
                                size_t consumed = scan_csv_buffer<N>(buf, valid_len, row_cb);
                                if (consumed < valid_len && buf_file_off + consumed < tk.end) {
                                    if (split_fields_slow<N>(SV(buf + consumed, valid_len - consumed), tail_f))
                                        row_cb(tail_f);
                                }
                                carry = 0;
                                buf_file_off = tk.file_size;
                                break;
                            }
                            // Rare: line > 512 bytes past tk.end. No '\n' exists in [limit, valid_len),
                            // so scanning [0, valid_len) only emits lines ending < limit and preserves
                            // the spanning tail in [consumed, valid_len) untouched!
                            std::memset(buf + valid_len, 0, 64);
                            size_t consumed = scan_csv_buffer<N>(buf, valid_len, row_cb);
                            carry = valid_len - consumed;
                            if (carry > 0) std::memmove(buf, buf + consumed, carry);
                            buf_file_off += consumed;
                        }
                    }
                }
                on_flush(wid);
                std::free(buf);
            } catch (...) {
                next.store(tasks_.size(), std::memory_order_relaxed);
                std::lock_guard lk(err_mu);
                if (!err) err = std::current_exception();
            }
        };

        global_pool().run(nworkers, worker);
        if (err) std::rethrow_exception(err);
    }
};

} // namespace blitz
