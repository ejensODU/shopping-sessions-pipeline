#pragma once
#include <algorithm>
#include <atomic>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ss {
namespace fs = std::filesystem;

template<class Function>
void parallel(size_t count, unsigned threads, Function task) {
    std::atomic<size_t> next{0};
    std::mutex error_lock;
    std::exception_ptr error;
    auto worker = [&](unsigned worker_id) {
        try {
            for (;;) {
                const size_t index = next.fetch_add(1, std::memory_order_relaxed);
                if (index >= count) break;
                task(worker_id, index);
            }
        } catch (...) {
            next.store(count, std::memory_order_relaxed);
            std::lock_guard guard(error_lock);
            if (!error) error = std::current_exception();
        }
    };
    std::vector<std::thread> pool;
    for (unsigned i = 1; i < std::min<size_t>(threads, count); ++i)
        pool.emplace_back(worker, i);
    worker(0);
    for (auto& thread : pool) thread.join();
    if (error) std::rethrow_exception(error);
}

class MappedFile {
public:
    const char* data = nullptr;
    size_t size;
    std::atomic<unsigned> pending{0};

    explicit MappedFile(const fs::path& path) : size(fs::file_size(path)) {
        if (!size) return;
        int fd = open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) throw std::runtime_error("Could not open " + path.string());
        void* mapped = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (mapped == MAP_FAILED) throw std::runtime_error("Could not map " + path.string());
        data = static_cast<const char*>(mapped);
    }
    void release() { if (data) { munmap(const_cast<char*>(data), size); data = nullptr; } }
    ~MappedFile() { release(); }
};

class Input {
    struct Chunk { MappedFile* file; const char *start, *end; };
    std::vector<std::unique_ptr<MappedFile>> files_;
    std::vector<Chunk> chunks_;

    void add(const fs::path& path) {
        auto file = std::make_unique<MappedFile>(path);
        if (!file->size) return;
        const char* end = file->data + file->size;
        auto after_line = [end](const char* p) {
            auto nl = static_cast<const char*>(std::memchr(p, '\n', end - p));
            return nl ? nl + 1 : end;
        };
        constexpr size_t block = 1024 * 1024;
        const char* p = after_line(file->data); // exactly one header per file
        while (p < end) {
            const char* stop = size_t(end - p) > block ? after_line(p + block) : end;
            chunks_.push_back({file.get(), p, stop});
            ++file->pending;
            p = stop;
        }
        bytes += file->size;
        files_.push_back(std::move(file));
    }
public:
    size_t bytes = 0;
    explicit Input(const fs::path& path) {
        if (!fs::is_directory(path)) { add(path); return; }
        std::vector<fs::path> paths;
        for (const auto& f : fs::directory_iterator(path))
            if (f.is_regular_file() && f.path().extension() == ".csv") paths.push_back(f.path());
        // Assign contiguous chunks of the largest files early.
        std::sort(paths.begin(), paths.end(), [](auto& a, auto& b) { return fs::file_size(a) > fs::file_size(b); });
        for (auto& p : paths) add(p);
    }
    // Retain maps only when consumers hold string views into the input.
    template<class Function>
    void read(unsigned threads, bool retain, Function row) {
        parallel(chunks_.size(), threads, [&](unsigned w, size_t i) {
            auto [file, p, end] = chunks_[i];
            while (p < end) {
                auto nl = static_cast<const char*>(std::memchr(p, '\n', end - p));
                auto stop = nl ? nl : end;
                row(w, std::string_view(p, stop-p));
                p = nl ? nl+1 : end;
            }
            if (!retain && file->pending.fetch_sub(1) == 1) file->release();
        });
    }
};
} // namespace ss
