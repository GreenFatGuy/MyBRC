#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string_view>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>
#include <immintrin.h>

#include <sys/mman.h>
#include <sys/stat.h>

static constexpr char DATA[] = "measurements.txt";
static constexpr char DELIM = ';';
static constexpr char NEW_LINE = '\n';
// static constexpr int MAX_NAMES = 10'000;

//#define DBG 1

#ifdef DBG
#define DBG_NOINLINE __attribute_noinline__ 
#else
#define DBG_NOINLINE
#endif

template <std::size_t Size>
struct DWordStr {
    static_assert(Size > 0);

    ~DWordStr() { if (ptr != nullptr) delete[] ptr; }
    uint64_t dwords[Size];
    const char* ptr = nullptr;

    constexpr bool is_small() const noexcept {
      return ptr == nullptr;
    }

    constexpr bool operator==(const DWordStr& other) const noexcept {
      const uint8_t m = static_cast<uint8_t>(is_small() << 0) &
                        static_cast<uint8_t>(other.is_small() << 1);
      switch (m) {
        [[likely]] case 0b11: {
          bool eq = true;
#pragma unroll
          for (std::size_t i = 0; i < Size; ++i)
            eq &= !(dwords[i] ^ other.dwords[i]);
          return eq;
        }
        case 0b10:
            [[fallthrough]];
        case 0b01:
            return false;
        case 0b00:
            return dwords[0] == dwords[1] && (std::memcmp(ptr, other.ptr, dwords[0]) == 0);
      }
      __builtin_unreachable();
    }
};

template <typename H> struct HashWrapper {
  constexpr std::size_t operator()(const std::string &s) const noexcept {
    return H::hash(s.data(), s.size());
  }

  constexpr std::size_t operator()(std::string_view s) const noexcept {
    return H::hash(s.data(), s.size());
  }

  constexpr std::size_t operator()(const char *s) const noexcept {
    return this->operator()(std::string_view(s));
  }

  constexpr std::size_t operator()(std::span<const char> s) const noexcept {
    return H::hash(s.data(), s.size());
  }

  template <std::size_t Size>
  constexpr std::size_t operator()(const DWordStr<Size>& str) const noexcept {
    if (str.is_small) [[ likely ]] {
      // Basically fnv8a
      std::size_t h = 14695981039346656037ull;
#pragma unroll
      for (std::size_t i = 0; i < Size; ++i)
        h ^= str.dwords * 14695981039346656037ull;
      return h; 
    } else {
      return H::hash(str.ptr, str.dwords[0]);
    }
  }
};



struct fnv1a {
  static constexpr std::size_t hash(const char *s, std::size_t n) noexcept {
    std::size_t h = 14695981039346656037ull;
    for (std::size_t i = 0; i < n; ++i) {
      h ^= static_cast<uint8_t>(s[i]);
      h *= 1099511628211ull;
    }
    return h;
  }
};

using FNV1A = HashWrapper<fnv1a>;

struct fnv8a {
  static constexpr std::size_t hash(const char *s, std::size_t n) noexcept {
    uint64_t h = 14695981039346656037ull;
    for (std::size_t i = n; i >= 8; i -= 8) {
      uint64_t x = 0;
      std::memcpy(&x, s, sizeof(x));
      h ^= x;
      h *= 1099511628211ull;
      s += 8;
      n -= 8;
    }
    switch (n & 7) {
      case 7:
        h ^= static_cast<uint8_t>(*s++);
        h *= 1099511628211ull;
      case 6:
        h ^= static_cast<uint8_t>(*s++);
        h *= 1099511628211ull;
      case 5:
        h ^= static_cast<uint8_t>(*s++);
        h *= 1099511628211ull;
      case 4:
        h ^= static_cast<uint8_t>(*s++);
        h *= 1099511628211ull;
      case 3:
        h ^= static_cast<uint8_t>(*s++);
        h *= 1099511628211ull;
      case 2:
        h ^= static_cast<uint8_t>(*s++);
        h *= 1099511628211ull;
      case 1:
        h ^= static_cast<uint8_t>(*s++);
        h *= 1099511628211ull;
    }
    return h;
  }
};

using FNV8A = HashWrapper<fnv8a>;

struct Data {
  int16_t min = std::numeric_limits<int16_t>::max();
  int16_t max = std::numeric_limits<int16_t>::min();
  uint32_t count = 0;
  int64_t sum = 0;
};

template <std::size_t BUCKETS = 8192, std::size_t CHAIN_START_SIZE = 2,
          typename Hash = FNV1A>
class MyHashMap {
public:
  struct KV {
    std::string name;
    Data data;
  };
  using Bucket = std::vector<KV>;

  Data &try_emplace(std::string_view name, Data &&v) {
    const std::size_t h = Hash()(name);
    auto &b = map_[h & (BUCKETS - 1)];

    auto it = std::find_if(b.begin(), b.end(),
                           [&](auto &p) { return p.name == name; });

    if (it == b.end()) [[unlikely]] {
      if (b.empty())
        b.reserve(CHAIN_START_SIZE);

      b.emplace_back(KV{std::string(name), std::forward<Data>(v)});
      ++size_;
      return b.back().data;
    } else {
      return it->data;
    }
  }

  std::size_t size() const noexcept { return size_; }

  std::vector<KV> to_plain() const {
    std::vector<KV> v;
    v.reserve(size());
    for (const auto &b : map_) {
      for (const auto &kv : b) {
        v.emplace_back(kv);
      }
    }
    std::sort(v.begin(), v.end(),
              [](auto &a, auto &b) { return a.name < b.name; });
    return v;
  }

  void print_stats() const {
    std::array<int, BUCKETS> loads{};
    for (int i = 0; i < map_.size(); ++i) {
      loads[i] = map_[i].size();
    }
    int64_t sum = 0;
    int min = std::numeric_limits<int>::max();
    int max = std::numeric_limits<int>::min();

    for (int l : loads) {
      sum += l;
      min = std::min(l, min);
      max = std::max(l, max);
    }

    std::vector<int> counts;
    counts.resize(max + 1);
    for (int l : loads)
      counts[l]++;

    std::cerr << "MyHashMap stats load factor:\n";
    std::cerr << "\tmean chain length: "
              << static_cast<double>(sum) / loads.size() << "\n";
    std::cerr << "\tmin chain length:  " << min << "\n";
    std::cerr << "\tmax chain length:  " << max << "\n";

    std::cerr << "Chains stats:\n";
    for (int i = 0; i < counts.size(); ++i)
      std::cerr << "\t" << i << " -> " << counts[i] << "\n";
  }

private:
  std::array<Bucket, BUCKETS> map_{};
  std::size_t size_{0};
};

template <std::size_t BUCKETS, typename Hash = FNV1A, bool DEBUG = false>
class MyFlatHashMap {
public:
  static_assert((BUCKETS & (BUCKETS - 1)) == 0, "BUCKETS must be a power of 2");
  static_assert(BUCKETS > 0, "BUCKETS must be greater than 0");
  static constexpr auto idx = [](std::size_t h) { return h & (BUCKETS - 1); };

  struct KV {
    std::string name;
    std::size_t size;
    Data data{};
  };

  DBG_NOINLINE Data &try_emplace(std::span<const char> name) {
    const std::size_t h = Hash()(name);
    std::size_t s = idx(h);
    for (std::size_t i = 0; i < BUCKETS; ++i) {
      auto& e = map_[s]; 
      if (e.size == name.size() && (std::memcmp(name.data(), e.name.data(), e.size) == 0)) [[likely]] {
        if constexpr (DEBUG)
          hops_[i]++;

        return map_[s].data;
      }

      if (e.name.empty()) {
        if constexpr (DEBUG)
          hops_[i]++;

        e.name = std::string(std::string_view(name.data(), name.size()));
        e.size = name.size();
        ++size_;
        return e.data;
      }

      s = idx(s + 1);
    }
    __builtin_unreachable();
  }

  std::vector<std::pair<std::string, Data>> to_plain() const {
    std::vector<std::pair<std::string, Data>> v;
    v.reserve(size_);
    for (const auto &kv : map_) {
      if (!kv.name.empty())
        v.emplace_back(kv.name, kv.data);
    }
    std::sort(v.begin(), v.end(),
              [](auto &a, auto &b) { return a.first < b.first; });
    return v;
  }

  void print_stats() const {
    std::cerr << "Result stats:\n";
    for (std::size_t i = 0; i < hops_.size(); ++i) {
      if (hops_[i] == 0)
        continue;
      std::cerr << "hops[" << i << "] = " << hops_[i] << "\n";
    }
  }

  template <bool dbg>
  void merge(const MyFlatHashMap<BUCKETS, Hash, dbg> &other) {
    for (std::size_t i = 0; i < other.map_.size(); ++i) {
      if (other.map_[i].name.empty())
        continue;

      auto &d = try_emplace(other.map_[i].name);
      d.min = std::min(other.map_[i].data.min, d.min);
      d.max = std::max(other.map_[i].data.max, d.max);
      d.sum += other.map_[i].data.sum;
      d.count += other.map_[i].data.count;
    }
  }

private:
  std::array<KV, BUCKETS> map_{};
  std::size_t size_{0};
  std::array<std::size_t, BUCKETS> hops_{};
};

using Result = MyFlatHashMap<32768, FNV8A, false>;

void print_results(const Result &r) {
  std::cout << std::fixed << std::setprecision(1) << "{";
  bool first = true;

  auto v = r.to_plain();

  for (auto &&[name, data] : v) {
    if (!first)
      std::cout << ", ";

    std::cout << name << "=" << (static_cast<double>(data.min) / 10.0) << "/"
              << (static_cast<double>(data.sum) / 10.0 / data.count) << "/"
              << (static_cast<double>(data.max) / 10.0);
    first = false;
  }
  std::cout << "}\n";

  r.print_stats();
}

void update_result(Result &r, std::span<const char> name, int16_t t) {
  auto &s = r.try_emplace(name);

  s.max = std::max(t, s.max);
  s.min = std::min(t, s.min);
  s.count++;
  s.sum += t;
}

void panic(const char *err) {
  std::cerr << err << "\n";
  std::terminate();
}

int do_open(const char *file) {
  const int ret = ::open(file, O_RDONLY);
  if (ret < 0)
    panic("err open file");
  return ret;
}

void do_close(int fd) {
  int ret = ::close(fd);
  if (ret < 0)
    panic("err close file");
}

size_t get_size(int fd) {
  struct stat stat;
  const int ret = ::fstat(fd, &stat);
  if (ret < 0)
    panic("err fstat");
  return stat.st_size;
}

void *do_mmap(int fd, size_t size, int opt, int flag) {
  void *ptr = ::mmap(nullptr, size, opt, flag, fd, 0);
  if (ptr == MAP_FAILED)
    panic("err mmap");
  return ptr;
}

void do_unmap(void *ptr, size_t size) {
  const int ret = ::munmap(ptr, size);
  if (ret < 0)
    panic("err munmap");
}

void do_madvise(void *ptr, size_t size, int adv) {
  const int ret = ::madvise(ptr, size, adv);
  if (ret < 0)
    panic("err madvise");
}

class MMappedFile {
public:
  // Note: we mmap a 128bytes more. Because line is 100 + 1 + 5 + 1 = 107 bytes at max.
  // So we would read with SIMD by 128 bytes blocks (actually 2 * 64) without SIGSEGV.
  // Reading those extra bytes is legal (writing not), and they are zeros.
  MMappedFile(const char *file)
      : fd_(do_open(file)), size_(get_size(fd_)),
        ptr_(do_mmap(fd_, size_ + 128, PROT_READ, MAP_PRIVATE)) {
    do_madvise(ptr_, size_, MADV_SEQUENTIAL | MADV_WILLNEED | MADV_HUGEPAGE);
  }

  ~MMappedFile() {
    do_unmap(ptr_, size_);
    do_close(fd_);
  }

  void *ptr() { return ptr_; }
  size_t size() const { return size_; }

private:
  const int fd_;
  const size_t size_;
  void *ptr_;
};

struct temp {
  int16_t t;
  int8_t l;
};

constexpr DBG_NOINLINE temp parse_value(const char *d) {
  // 4 possible cases:
  // 1. 1.2
  // 2. -1.2
  // 3. 12.3
  // 4. -12.3

  int8_t neg = d[0] == '-';

  // skip sign, so d points to "positive" part of the number. We down to 2
  // cases:
  // 1. 1.2
  // 3. 12.3
  d += neg;

  // d0 is always the first digit of the number
  int8_t d0 = d[0] - '0';

  // in case 1 dot is 1, in case 3 dot is 0
  int8_t dot = d[1] == '.';

  // if dot is found (dot == 1), then d1 = d[2] - '0' (1.2 case)
  // if dot is not found (dot == 0), then d1 = d[1] - '0' (12.3 case)
  int8_t d1 = d[1 + dot] - '0';

  // if dot is found (dot == 1), we don't need d2, so it equals to d1 = d[2] -
  // '0' (1.2 case) if dot is not found (dot == 0), then d2 = d[3] - '0' (12.3
  // case)
  int8_t d2 = d[3 - dot] - '0';

  // min length is 3, +1 for sign and +1 if we did not find early dot
  int8_t len = 3 + neg + (1 - dot);
  // if dot is found, val = 10 * d0 + d1
  // else val = 100 * d0 + 10 * d1 + d2
  int16_t v_small = 10 * static_cast<int16_t>(d0) + static_cast<int16_t>(d1);
  int16_t val = (1 - 2 * neg) * (v_small * (10 - 9 * dot) +
                                 (1 - dot) * static_cast<int16_t>(d2));
  return {val, len};
}

namespace {

static constexpr char test1[] = "1.2";
static_assert(parse_value(test1).t == 12l);
static_assert(parse_value(test1).l == 3);

static constexpr char test2[] = "-1.2";
static_assert(parse_value(test2).t == -12);
static_assert(parse_value(test2).l == 4);

static constexpr char test3[] = "12.3";
static_assert(parse_value(test3).t == 123);
static_assert(parse_value(test3).l == 4);

static constexpr char test4[] = "-12.3";
static_assert(parse_value(test4).t == -123);
static_assert(parse_value(test4).l == 5);

} // namespace

enum class Flavour {
    MEMCHR,
    AVX2,
    AVX512,
};

template <Flavour F>
const char* find_next_byte_128(const char* ptr, char c) {
    if constexpr (F == Flavour::MEMCHR) {
      return static_cast<const char *>(std::memchr(ptr, c, 128l));
    } else if constexpr (F == Flavour::AVX2) {
        const __m256i needle = _mm256_set1_epi8(c);
        for (std::size_t offset = 0; offset < 128; offset += 32) {
            const __m256i chunk = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr + offset));
            const __m256i cmp = _mm256_cmpeq_epi8(chunk, needle);
            const uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(cmp));
            if (mask) [[ likely ]] {
                const std::size_t bit_index = _tzcnt_u32(mask);
                return ptr + offset + bit_index;
            }
        }
        return nullptr;
    } else if constexpr (F == Flavour::AVX512) {
        const __m512i needle = _mm512_set1_epi8(c);
        for (int offset = 0; offset < 128; offset += 64) {
            const __m512i chunk = _mm512_loadu_si512(reinterpret_cast<const void*>(ptr + offset));
            const __mmask64 mask = _mm512_cmpeq_epi8_mask(chunk, needle);
            if (mask) [[ likely ]] {
                const unsigned bit_index = static_cast<unsigned>(_tzcnt_u64(mask));
                return ptr + offset + bit_index;
            }
        }
        return nullptr;
    } else {
        static_assert(false);
    }
}

constexpr Flavour current_flavour() {
    // Note: MEMCHR currently better, stick with it
    return Flavour::MEMCHR;
#ifdef __AVX512F__
    return Flavour::AVX512;
#endif
#ifdef __AVX2__
    return Flavour::AVX2;
#endif
    return Flavour::MEMCHR;
}



using FileChunks = std::vector<std::span<const char>>;


FileChunks split_into_chunks(std::span<const char> mem, std::size_t min_chunk_size) {
  FileChunks chunks;
  chunks.reserve((mem.size() + min_chunk_size - 1) / min_chunk_size);

  while (!mem.empty()) {
    const std::size_t chunk_size = std::min(min_chunk_size, mem.size());
    const char *true_end = static_cast<const char *>(
        std::memchr(&mem[chunk_size], NEW_LINE, mem.size() - chunk_size));
    const std::size_t true_size = true_end == nullptr
        ? chunk_size
        : std::distance(mem.data(), true_end) + 1;
    chunks.emplace_back(mem.data(), true_size);


    mem = mem.subspan(true_size);
    //std::cerr << "Chunk: from=" << (uintptr_t)mem.data() << ", size=" << true_size << ", left=" << mem.size() << "\n";
  }
  return chunks;
}

DBG_NOINLINE void process_batch(Result &r, std::span<const char> batch) {

  constexpr std::size_t Mb = 1 << 20; 
  constexpr std::size_t unroll = 4;

  auto chunks = split_into_chunks(batch, 4*Mb);
  // round up to 8
  std::bitset<unroll> flag;
  std::array<std::size_t, unroll> idxs = []<auto... I>(std::index_sequence<I...>) {
    return std::array<std::size_t, unroll>{I...};
  }(std::make_index_sequence<unroll>());

  while (!flag.all()) {
#pragma unroll
    for (std::size_t j = 0; j < unroll; j++) {
      if (idxs[j] >= chunks.size()) [[unlikely]] {
        flag.set(j);
        continue;
      }

      auto& chunk = chunks[idxs[j]];
      const char *n = static_cast<const char *>(std::memchr(chunk.data(), DELIM, chunks.size()));
      const std::size_t name_len = std::distance(chunk.data(), n);
      const auto [val, len] = parse_value(n + 1);

      const auto name = std::span<const char>(chunk.data(), name_len);
      update_result(r, name, val);
      chunk = chunk.subspan(name_len + 1 + len + 1);

      idxs[j] = chunk.empty() ? idxs[j] + unroll : idxs[j];
    }
  }
}

void set_cpu_affinity(int cpu) {
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);
  CPU_SET(cpu, &cpu_set);
  auto self = pthread_self();
  const int ret = pthread_setaffinity_np(self, sizeof(cpu_set), &cpu_set);
  if (ret < 0)
    panic("err cpu affinity");
}

static constexpr std::size_t CHUNK = 10 * 1024 * 1024; // 10 Mb
//static constexpr std::size_t WORKERS = 15;

 DBG_NOINLINE void worker_routine(const FileChunks& chunks, Result& r, std::atomic<std::size_t>& next_chunk, int idx) {
  set_cpu_affinity(idx);
  std::size_t cur_chunk = idx;
  const std::size_t n_chunks = chunks.size();
  while (cur_chunk < n_chunks) {
    process_batch(r, chunks[cur_chunk]);
    cur_chunk = next_chunk.fetch_add(1, std::memory_order_relaxed);
  }
}

Result run_workers(std::size_t workers_count, const char *f_begin,
                   std::size_t f_size) {

  std::vector<std::thread> workers;
  workers.reserve(workers_count);
  std::vector<Result> results;
  results.resize(workers_count);

  auto chunks = split_into_chunks(std::span<const char>(f_begin, f_size), CHUNK);
  std::atomic<std::size_t> next_chunk{workers_count+1};

  for (std::size_t i = 0; i < workers_count; ++i) {
    workers.push_back(std::thread([&, i]() {
      worker_routine(chunks, results[i], next_chunk, i);
    }));
  }

  Result r{};
  worker_routine(chunks, r, next_chunk, workers_count);

  for (std::size_t i = 0; i < workers_count; ++i) {
    workers[i].join();
    r.merge(results[i]);
  }
  return r;
}

int main() {
  MMappedFile f(DATA);

  const char *ptr = static_cast<const char *>(f.ptr());
  const std::size_t space = f.size();

  void* rptr = do_mmap(-1, sizeof(Result), PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE);
  do_madvise(rptr, sizeof(Result), MADV_WILLNEED | MADV_HUGEPAGE);
  Result* stats = new (rptr) Result();
  process_batch(*stats, std::span<const char>(ptr, space));
  print_results(*stats);
  stats->~Result();
  do_unmap(rptr, sizeof(Result));

  return 0;
}
