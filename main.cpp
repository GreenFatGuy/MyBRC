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
#include <span>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <immintrin.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/stat.h>

static constexpr char DATA[] = "measurements.txt";
static constexpr char DELIM = ';';
static constexpr char NEW_LINE = '\n';
// static constexpr int MAX_NAMES = 10'000;

// #define DBG 1

#ifdef DBG
#define DBG_NOINLINE __attribute_noinline__
#else
#define DBG_NOINLINE
#endif

template <std::size_t Size> struct DWordStr {
  std::array<uint64_t, Size> dwords{};
  const char *ptr = nullptr;

  static_assert(Size > 0);

  DWordStr() { std::memset(dwords.data(), 0, sizeof(uint64_t) * Size); }

  ~DWordStr() {
    if (ptr != nullptr)
      delete ptr;
  }

  DWordStr(DWordStr &&other) {
    dwords = other.dwords;
    std::swap(ptr, other.ptr);
  }

  DWordStr(const DWordStr &other) {
    dwords = other.dwords;
    if (!other.is_small()) {
      char *ptr = new char[other.dwords[0]];
      std::memcpy(ptr, other.ptr, other.dwords[0]);
      this->ptr = ptr;
    }
  }

  DWordStr &operator=(DWordStr &&other) {
    if (this != &other) {
      this->~DWordStr();
      dwords = other.dwords;
      ptr = std::exchange(other.ptr, nullptr);
    }
    return *this;
  }

  DWordStr &operator=(const DWordStr &other) {
    if (this != &other) {
      this->~DWordStr();
      dwords = other.dwords;
      if (!other.is_small()) {
        char *ptr = new char[other.dwords[0]];
        std::memcpy(ptr, other.ptr, other.dwords[0]);
        this->ptr = ptr;
      }
    }
    return *this;
  }

  constexpr bool is_small() const noexcept { return ptr == nullptr; }

  constexpr bool empty() const noexcept { return dwords[0] == 0; }

  constexpr bool operator==(const DWordStr &other) const noexcept {
    const uint8_t m = (static_cast<uint8_t>(is_small()) << 0) |
                      (static_cast<uint8_t>(other.is_small()) << 1);
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
    case 0b00: {
      return (dwords[0] == other.dwords[0]) &&
             (std::memcmp(ptr, other.ptr, dwords[0]) == 0);
    }
    }
    __builtin_unreachable();
  }

  std::string to_string() const {
    auto [str_ptr, size] = [this]() -> std::tuple<const char *, std::size_t> {
      if (is_small()) {
        return {reinterpret_cast<const char *>(dwords.data()),
                Size * sizeof(uint64_t)};
      } else {
        return {ptr, dwords[0]};
      }
    }();
    return std::string(std::string_view(str_ptr, size));
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
  constexpr std::size_t operator()(const DWordStr<Size> &str) const noexcept {
    if (str.is_small()) [[likely]] {
      // Basically fnv8a
      std::size_t h = 0;
#pragma unroll
      for (std::size_t i = 0; i < Size; ++i) {
        h ^= str.dwords[i];
      }
      h *= 14695981039346656037ull;
      h >>= 35;
      return h;
    } else {
      return H::hash(str.ptr, str.dwords[0]);
    }
  }
};

struct fnv8a {
  static constexpr std::size_t hash(const char *s, std::size_t n) noexcept {
    uint64_t h = 0;
    for (std::size_t i = n; i >= 8; i -= 8) {
      uint64_t w = 0;
      std::memcpy(&w, s, sizeof(w));
      h ^= w;
      s += 8;
      n -= 8;
    }
    if (n > 0) {
      uint64_t w = 0;
      std::memcpy(&w, s, n);
      h ^= w;
    }
    h *= 14695981039346656037ull;
    h >>= 35;
    return h;
  }
};

template <std::size_t BUCKETS, typename Key, typename Value, typename Hash,
          bool DEBUG = false>
class MyFlatHashMap {
public:
  static_assert((BUCKETS & (BUCKETS - 1)) == 0, "BUCKETS must be a power of 2");
  static_assert(BUCKETS > 0, "BUCKETS must be greater than 0");
  static constexpr auto idx = [](std::size_t h) { return h & (BUCKETS - 1); };

  struct KV {
    Key key{};
    Value value{};
  };

  constexpr std::size_t size() const noexcept { return size_; }

  constexpr bool empty() const noexcept { return size_ == 0; }

  DBG_NOINLINE Value &try_emplace(Key &&key) {
    const std::size_t h = Hash()(key);
    std::size_t s = idx(h);
    for (std::size_t i = 0; i < BUCKETS; ++i) {
      auto &e = map_[s];
      if (e.key == key) [[likely]] {
        if constexpr (DEBUG)
          hops_[i]++;
        return map_[s].value;
      }

      if (e.key.empty()) {

        if constexpr (DEBUG)
          hops_[i]++;

        e.key = std::forward<Key>(key);
        ++size_;
        return e.value;
      }

      s = idx(s + 1);
    }
    __builtin_unreachable();
  }

  template <typename F> void for_each(F &&f) const {
    for (const auto &kv : map_) {
      if (!kv.key.empty())
        f(kv);
    }
  }

  void print_stats() const {
    if constexpr (DEBUG) {
      std::cout << "Result stats:\n";
      for (std::size_t i = 0; i < hops_.size(); ++i) {
        if (hops_[i] == 0)
          continue;
        std::cout << "hops[" << i << "] = " << hops_[i] << "\n";
      }
    }
  }

private:
  std::array<KV, BUCKETS> map_{};
  std::size_t size_{0};
  std::array<std::size_t, BUCKETS> hops_{};
};

using FNV8A = HashWrapper<fnv8a>;

struct Data {
  int16_t min = std::numeric_limits<int16_t>::max();
  int16_t max = std::numeric_limits<int16_t>::min();
  uint32_t count = 0;
  int64_t sum = 0;
};

using Name = DWordStr<2>;

using Result = MyFlatHashMap<32768, Name, Data, FNV8A, true>;

void merge(Result &left, const Result &right) {
  right.for_each([&](const Result::KV &kv) {
    // TODO: this is very stupid, but does not matter so much
    auto cpy = kv.key;
    auto &d = left.try_emplace(std::move(cpy));
    d.min = std::min(kv.value.min, d.min);
    d.max = std::max(kv.value.max, d.max);
    d.sum += kv.value.sum;
    d.count += kv.value.count;
  });
}

std::vector<std::pair<std::string, Data>> to_plain(const Result &r) {
  std::vector<std::pair<std::string, Data>> v;
  v.reserve(r.size());
  r.for_each([&](const Result::KV &kv) {
    v.emplace_back(kv.key.to_string(), kv.value);
  });
  std::sort(v.begin(), v.end(),
            [](auto &a, auto &b) { return a.first < b.first; });
  return v;
}

void print_results(const Result &r) {
  std::cout << std::fixed << std::setprecision(1) << "{";
  bool first = true;

  auto v = to_plain(r);

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

void update_result(Result &r, DWordStr<2> &&name, int16_t t) {
  auto &s = r.try_emplace(std::forward<DWordStr<2>>(name));

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
  // Note: we mmap a 128bytes more. Because line is 100 + 1 + 5 + 1 = 107 bytes
  // at max. So we would read with SIMD by 128 bytes blocks (actually 2 * 64)
  // without SIGSEGV. Reading those extra bytes is legal (writing not), and they
  // are zeros.
  static constexpr std::size_t EXTRA_PAD = 128ul;
  MMappedFile(const char *file)
      : fd_(do_open(file)), size_(get_size(fd_)),
        ptr_(do_mmap(fd_, (size_ + EXTRA_PAD), PROT_READ, MAP_PRIVATE)) {
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

template <Flavour F> const char *find_next_byte_128(const char *ptr, char c) {
  if constexpr (F == Flavour::MEMCHR) {
    return static_cast<const char *>(std::memchr(ptr, c, 128l));
  } else if constexpr (F == Flavour::AVX2) {
    const __m256i needle = _mm256_set1_epi8(c);
    for (std::size_t offset = 0; offset < 128; offset += 32) {
      const __m256i chunk =
          _mm256_loadu_si256(reinterpret_cast<const __m256i *>(ptr + offset));
      const __m256i cmp = _mm256_cmpeq_epi8(chunk, needle);
      const uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(cmp));
      if (mask) [[likely]] {
        const std::size_t bit_index = _tzcnt_u32(mask);
        return ptr + offset + bit_index;
      }
    }
    return nullptr;
  } else if constexpr (F == Flavour::AVX512) {
    const __m512i needle = _mm512_set1_epi8(c);
    for (int offset = 0; offset < 128; offset += 64) {
      const __m512i chunk =
          _mm512_loadu_si512(reinterpret_cast<const void *>(ptr + offset));
      const __mmask64 mask = _mm512_cmpeq_epi8_mask(chunk, needle);
      if (mask) [[likely]] {
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
  // Note: MEMCHR is currently better that all my SIMD tries, stick with it
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

FileChunks split_into_chunks(std::span<const char> mem,
                             std::size_t chunk_size) {
  FileChunks chunks;
  chunks.reserve((mem.size() + chunk_size - 1) / chunk_size);
  std::size_t next_chunk_size = chunk_size;
  while (!mem.empty()) {
    const std::size_t chunk_size = std::min(next_chunk_size, mem.size());

    const char *true_end = static_cast<const char *>(
        std::memchr(&mem[chunk_size], NEW_LINE, mem.size() - chunk_size));
    const std::size_t true_size = true_end == nullptr
                                      ? chunk_size
                                      : std::distance(mem.data(), true_end) + 1;
    chunks.emplace_back(mem.data(), true_size);
    // Note: balancing chunks to preserve total size
    // - if true_size is equal to chunk size - next will be ~chunk_size
    // - if true_size is less than chunk size - next will be slightly greater
    // - if true_size is greater than chunk_size - netx will be slightly less
    next_chunk_size = 2 * chunk_size - true_size;

    mem = mem.subspan(true_size);
  }
  return chunks;
}

struct NameParseRes {
  DWordStr<2> name;
  uint8_t len;
};

NameParseRes parse_name_simd_reg(const char *ptr) {
  static_assert(DELIM == 0x3B);
  constexpr uint64_t MASK1 = 0x0101010101010101ull;
  constexpr uint64_t MASK2 = 0x8080808080808080ull;
  constexpr uint64_t DELIM_MASK = MASK1 * DELIM;
  NameParseRes res{};
  std::memcpy(res.name.dwords.data(), ptr,
              sizeof(uint64_t) * res.name.dwords.size());

  std::array<uint64_t, 2> masks{res.name.dwords[0] ^ DELIM_MASK,
                                res.name.dwords[1] ^ DELIM_MASK};

  masks[0] = (masks[0] - MASK1) & ~masks[0] & MASK2;
  masks[1] = (masks[1] - MASK1) & ~masks[1] & MASK2;

  // Note: DELIM is in first 16 bytes
  if ((masks[0] | masks[1]) != 0) [[likely]] {
    const uint64_t is_in_second = (masks[0] == 0);
    masks[0] = static_cast<uint64_t>(__builtin_ctzll(masks[0]) >> 3),
    masks[1] = static_cast<uint64_t>((0x40 + __builtin_ctzll(masks[1])) >> 3);
    res.len = masks[is_in_second];

    __uint128_t keep128 = (static_cast<__uint128_t>(1) << (res.len << 3)) - 1;

    masks[0] = static_cast<uint64_t>(keep128);
    masks[1] = static_cast<uint64_t>(keep128 >> 64);

    res.name.dwords[0] &= masks[0];
    res.name.dwords[1] &= masks[1];
    // Fallback for longer name
  } else {
    const char *n = find_next_byte_128<current_flavour()>(ptr, DELIM);
    res.len = std::distance(ptr, n);
    res.name.dwords[0] = res.len;
    // TODO: this is actually not neccessary here
    //       but DWordStrView type needed;
    char *new_ptr = new char[res.len];
    std::memcpy(new_ptr, ptr, res.len);
    res.name.ptr = new_ptr;
  }
  return res;
}

DBG_NOINLINE void process_batch(Result &r, std::span<const char> batch) {

  constexpr std::size_t Mb = 1 << 20;
  constexpr std::size_t unroll = 4;

  auto chunks = split_into_chunks(batch, 4 * Mb);
  std::bitset<unroll> all_finished;
  std::array<std::size_t, unroll> idxs =
      []<auto... I>(std::index_sequence<I...>) {
        return std::array<std::size_t, unroll>{I...};
      }(std::make_index_sequence<unroll>());

  while (!all_finished.all()) {
#pragma unroll
    for (std::size_t j = 0; j < unroll; j++) {
      if (idxs[j] >= chunks.size()) [[unlikely]] {
        all_finished.set(j);
        continue;
      }

      auto &chunk = chunks[idxs[j]];

      auto [name, name_len] = parse_name_simd_reg(chunk.data());
      auto [val, val_len] = parse_value(&chunk[name_len + 1]);

      update_result(r, std::move(name), val);

      chunk = chunk.subspan(name_len + 1 + val_len + 1);
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

DBG_NOINLINE void worker_routine(const FileChunks &chunks, Result &r,
                                 std::atomic<std::size_t> &next_chunk,
                                 int idx) {
  set_cpu_affinity(idx);
  std::size_t cur_chunk = idx;
  const std::size_t n_chunks = chunks.size();
  while (cur_chunk < n_chunks) {
    process_batch(r, chunks[cur_chunk]);
    cur_chunk = next_chunk.fetch_add(1, std::memory_order_relaxed);
  }
}

Result run_workers(std::size_t workers_count, std::span<const char> file) {
  static constexpr std::size_t CHUNK = 32 * 1024 * 1024; // 32 Mb

  std::vector<std::thread> workers;
  std::vector<Result> results;
  workers.reserve(workers_count);
  results.resize(workers_count);

  auto chunks = split_into_chunks(file, CHUNK);
  std::atomic<std::size_t> next_chunk{workers_count + 1};

  for (std::size_t i = 0; i < workers_count; ++i) {
    workers.push_back(std::thread(
        [&, i]() { worker_routine(chunks, results[i], next_chunk, i); }));
  }

  Result r{};
  worker_routine(chunks, r, next_chunk, workers_count);

  for (std::size_t i = 0; i < workers_count; ++i) {
    workers[i].join();
    merge(r, results[i]);
  }
  return r;
}

static constexpr bool MULTITHREADED = true;
static constexpr std::size_t WORKERS = 15;

int main() {
  MMappedFile file(DATA);

  auto file_mem =
      std::span<const char>(static_cast<const char *>(file.ptr()), file.size());

  auto stats = [&]() {
    if constexpr (MULTITHREADED) {
      return run_workers(WORKERS, file_mem);
    } else {
      Result stats;
      process_batch(stats, file_mem);
      return stats;
    }
  }();

  print_results(stats);

  return 0;
}
