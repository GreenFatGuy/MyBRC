#include <algorithm>
#include <array>
#include <atomic>
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
static constexpr std::size_t Kb = 1 << 10;
static constexpr std::size_t Mb = Kb << 10;

// #define DBG

#ifdef DBG
#define DBG_NOINLINE __attribute_noinline__
#else
#define DBG_NOINLINE
#endif

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

struct DoubleQWordStr {
  static constexpr uint8_t TAG_BIG = 0xFF;
  static constexpr uint8_t TAG_SMALL = 0x00;
  static constexpr uint64_t BIG_SIZE_MASK = 0x00FFFFFFFFFFFFFF;

  union {
      std::array<uint64_t, 2> small{};
      uint8_t raw[16];
      struct {
        const char* str;
        std::size_t size;
      } big;
    } data;

  DoubleQWordStr() { std::memset(&data, 0, sizeof(data)); }

  ~DoubleQWordStr() {
    if (!is_small() && data.big.str != nullptr)
      delete[] data.big.str;
  }

  DoubleQWordStr(DoubleQWordStr &&other) {
    std::memcpy(&data, &other.data, sizeof(data));
    other.data.big.str = nullptr;
  }

  DoubleQWordStr(const DoubleQWordStr &other) {
    char* ptr = nullptr;
    if (!other.is_small()) {
      const std::size_t size = other.big_size();
      ptr = new char[size];
      std::memcpy(ptr, other.data.big.str, size);
    }
    std::memcpy(&data, &other.data, sizeof(data));
    if (ptr != nullptr)
      data.big.str = ptr;
  }

  DoubleQWordStr &operator=(DoubleQWordStr &&other) {
    if (this != &other) {
      this->~DoubleQWordStr();
      new (this) DoubleQWordStr(std::forward<DoubleQWordStr>(other));
    }
    return *this;
  }

  DoubleQWordStr &operator=(const DoubleQWordStr &other) {
    if (this != &other) {
      this->~DoubleQWordStr();
      new (this) DoubleQWordStr(other);
    }
    return *this;
  }

  constexpr bool is_small() const noexcept { return data.raw[15] == TAG_SMALL; }

  constexpr bool empty() const noexcept { return data.small[0] == 0; }

  constexpr std::size_t big_size() const noexcept { return data.big.size & BIG_SIZE_MASK; }

  constexpr DBG_NOINLINE bool operator==(const DoubleQWordStr &other) const noexcept {
    const uint8_t m = (static_cast<uint8_t>(is_small()) << 0) |
                      (static_cast<uint8_t>(other.is_small()) << 1);
    switch (m) {
    [[likely]] case 0b11: {
      return static_cast<uint8_t>(!(data.small[0] ^ other.data.small[0])) &
             static_cast<uint8_t>(!(data.small[1] ^ other.data.small[1]));
    }
    case 0b10:
      [[fallthrough]];
    case 0b01:
      return false;
    case 0b00:
      return (data.big.size == other.data.big.size) &&
             (std::memcmp(data.big.str, other.data.big.str, big_size()) == 0);
    }
    __builtin_unreachable();
  }

  std::string to_string() const {
    auto [str_ptr, size] = [this]() -> std::tuple<const char *, std::size_t> {
      if (is_small()) {
        std::size_t len = 0;
        while (len < sizeof(data.raw) && data.raw[len] != '\0')
          ++len;
        return {reinterpret_cast<const char*>(data.raw), len};
      } else {
        return {data.big.str, big_size()};
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

  constexpr std::size_t operator()(const DoubleQWordStr &str) const noexcept {
    return H::hash(str);
  }
};

struct MyHash {

  static constexpr std::size_t murmur64(std::size_t h) noexcept {
    h ^= h >> 33;
    h *= 0xff51afd7ed558ccdull;
    h ^= h >> 33;
    h *= 0xc4ceb9fe1a85ec53ull;
    h ^= h >> 33;
    return h;
  }

  static constexpr std::size_t hash(const char *s, std::size_t n) noexcept {
    uint64_t h = 0xcbf29ce484222325ull;
    for (std::size_t i = n; i >= 8; i -= 8) {
      uint64_t w = 0;
      std::memcpy(&w, s, sizeof(w));
      h ^= w;
      s += 8;
      n -= 8;
    }
    uint64_t w = 0;
    std::memcpy(&w, s, n);
    h ^= w;
    return murmur64(h);
  }

  static constexpr std::size_t hash(const DoubleQWordStr &str) noexcept {
    if (str.is_small()) [[likely]] {
      // Basically fnv8a
      std::size_t h = 0xcbf29ce484222325ull;
      h ^= str.data.small[0];
      h ^= str.data.small[1];
      return murmur64(h);
    } else {
      return hash(str.data.big.str, str.big_size());
    }
  }
};

template <std::size_t BUCKETS, typename Key, typename Value, typename Hash,
          bool DEBUG = false>
class MyFlatHashMap {
public:
  static_assert((BUCKETS & (BUCKETS - 1)) == 0, "BUCKETS must be a power of 2");
  static_assert(BUCKETS > 0, "BUCKETS must be greater than 0");
  static constexpr auto idx = [](std::size_t h) { return h & (BUCKETS - 1); };

  MyFlatHashMap() : map_(nullptr) {
    auto *ptr = do_mmap(-1, BUCKETS * sizeof(KV), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE);
    map_ = static_cast<KV*>(ptr);
  }

  ~MyFlatHashMap() {
    if (map_)
      do_unmap(map_, sizeof(KV) * BUCKETS);
  }

  struct KV {
    Key key{};
    Value value{};
  };

  DBG_NOINLINE Value &try_emplace(Key &&key) {
    const std::size_t h = Hash()(key);
    std::size_t s = idx(h);
    for (std::size_t i = 0; i < BUCKETS; ++i) {
      auto &e = map_[s];
      if (e.key == key) [[likely]] {
        return map_[s].value;
      }

      if (e.key.empty()) {
        e.key = std::forward<Key>(key);
        return e.value;
      }

      s = idx(s + 1);
    }
    __builtin_unreachable();
  }

  template <typename F> void for_each(F &&f) const {
    for (int i = 0; i < BUCKETS; ++i) {
      if (!map_[i].key.empty())
        f(map_[i]);
    }
  }

private:
  KV* map_;
};

using Hash = HashWrapper<MyHash>;

struct Data {
  int16_t min = std::numeric_limits<int16_t>::max();
  int16_t max = std::numeric_limits<int16_t>::min();
  uint32_t count = 0;
  int64_t sum = 0;
};

using Result = MyFlatHashMap<65536, DoubleQWordStr, Data, Hash>;
static_assert(sizeof(Result::KV) == 32);

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
  v.reserve(10000);
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
}

void update_result(Result &r, DoubleQWordStr &&name, int16_t t) {
  auto &s = r.try_emplace(std::forward<DoubleQWordStr>(name));

  s.max = std::max(t, s.max);
  s.min = std::min(t, s.min);
  s.count++;
  s.sum += t;
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

struct alignas(64) Chunk {
    const char* data;
    std::size_t size;
};

using FileChunks = std::vector<Chunk>;

FileChunks split_into_chunks(Chunk mem,
                             std::size_t chunk_size) {
  FileChunks chunks;
  chunks.reserve((mem.size + chunk_size - 1) / chunk_size);
  std::size_t next_chunk_size = chunk_size;
  while (mem.size != 0) {
    const std::size_t chunk_size = std::min(next_chunk_size, mem.size);

    const char *true_end = static_cast<const char *>(
        std::memchr(&mem.data[chunk_size], NEW_LINE, mem.size - chunk_size));
    const std::size_t true_size = true_end == nullptr
                                      ? chunk_size
                                      : std::distance(mem.data, true_end) + 1;
    chunks.emplace_back(mem.data, true_size);
    // Note: balancing chunks to preserve total size
    // - if true_size is equal to chunk size - next will be ~chunk_size
    // - if true_size is less than chunk size - next will be slightly greater
    // - if true_size is greater than chunk_size - netx will be slightly less
    next_chunk_size = 2 * chunk_size - true_size;

    mem.data += true_size;
    mem.size -= true_size;
  }
  return chunks;
}

struct NameParseRes {
  DoubleQWordStr name;
  uint8_t len;
};

#define KEEP_MASK(len) ((__uint128_t(1) << ((len) << 3)) - 1)
#define KEEP_MASK_UP(len) (static_cast<uint64_t>(KEEP_MASK(len) >> 64))
#define KEEP_MASK_LOW(len) (static_cast<uint64_t>(KEEP_MASK(len)))


NameParseRes parse_name_simd(const char* __restrict__ ptr) {
  static_assert(DELIM == 0x3B);
  static constexpr uint64_t KEEP_MASK[16][2] = {
    {0, 0},
    {KEEP_MASK_LOW(1), KEEP_MASK_UP(1)},
    {KEEP_MASK_LOW(2), KEEP_MASK_UP(2)},
    {KEEP_MASK_LOW(3), KEEP_MASK_UP(3)},
    {KEEP_MASK_LOW(4), KEEP_MASK_UP(4)},
    {KEEP_MASK_LOW(5), KEEP_MASK_UP(5)},
    {KEEP_MASK_LOW(6), KEEP_MASK_UP(6)},
    {KEEP_MASK_LOW(7), KEEP_MASK_UP(7)},
    {KEEP_MASK_LOW(8), KEEP_MASK_UP(8)},
    {KEEP_MASK_LOW(9), KEEP_MASK_UP(9)},
    {KEEP_MASK_LOW(10), KEEP_MASK_UP(10)},
    {KEEP_MASK_LOW(11), KEEP_MASK_UP(11)},
    {KEEP_MASK_LOW(12), KEEP_MASK_UP(12)},
    {KEEP_MASK_LOW(13), KEEP_MASK_UP(13)},
    {KEEP_MASK_LOW(14), KEEP_MASK_UP(14)},
    {KEEP_MASK_LOW(15), KEEP_MASK_UP(15)},
  };
  NameParseRes res{};

  const __m128i chunk =
      _mm_loadu_si128(reinterpret_cast<const __m128i *>(ptr));
  const __m128i delim_vec = _mm_set1_epi8(DELIM);
  const uint32_t mask =
      static_cast<uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(chunk, delim_vec)));

  if (mask) [[likely]] {
    const uint32_t len = std::countr_zero(mask);
    res.len = static_cast<uint8_t>(len);

    std::memcpy(res.name.data.small.data(), ptr,
                2 * sizeof(uint64_t));

    res.name.data.small[0] &= KEEP_MASK[len][0];
    res.name.data.small[1] &= KEEP_MASK[len][1];
    res.name.data.raw[15] = DoubleQWordStr::TAG_SMALL;
  } else {
    const char *n = find_next_byte_128<current_flavour()>(ptr, DELIM);
    res.len = static_cast<uint8_t>(std::distance(ptr, n));
    res.name.data.big.size = res.len;
    char *new_ptr = new char[res.len];
    std::memcpy(new_ptr, ptr, res.len);
    res.name.data.big.str = new_ptr;
    res.name.data.raw[15] = DoubleQWordStr::TAG_BIG;
  }
  return res;
}

template <std::size_t UNROLL = 8, std::size_t CHUNK = 2 * Mb>
DBG_NOINLINE void process_batch(Result &r, Chunk batch) {
  struct {
    const char* begin;
    const char* end;
  } lanes[UNROLL];

  const std::size_t lane_size = batch.size / UNROLL;
  const char *batch_end = batch.data + batch.size;
  const char *ptr = batch.data;
  for (int i = 0; i < UNROLL; ++i) {
    lanes[i].begin = ptr;
    if (i == UNROLL - 1) {
      lanes[i].end = batch_end;
      break;
    }
    ptr += lane_size;
    const char *end = static_cast<const char *>(
        std::memchr(ptr, NEW_LINE, std::distance(ptr, batch_end)));
    ptr = end == nullptr ? ptr : end + 1;
    lanes[i].end = ptr;
  }

  uint64_t lanes_state = 0;
  constexpr uint64_t ALL_DONE = (1ull << UNROLL) - 1;
  static_assert(UNROLL < 64);
  while (lanes_state != ALL_DONE) {
#pragma unroll
    for (std::size_t j = 0; j < UNROLL; j++) {
      auto& lane = lanes[j];
      if (lane.begin >= lane.end) [[unlikely]] {
        lanes_state |= 1ull << j;
        continue;
      }

      auto [name, name_len] = parse_name_simd(lane.begin);
      auto [val, val_len] = parse_value(lane.begin + name_len + 1);
      update_result(r, std::move(name), val);

      const std::size_t len = name_len + 1 + val_len + 1;
      lane.begin += len;
    }
  }
}

void set_cpu_affinity(int cpu) {
  cpu_set_t cpu_set;
  CPU_ZERO(&cpu_set);
  CPU_SET(cpu, &cpu_set);
  auto self = pthread_self();
  const int ret = pthread_setaffinity_np(self, sizeof(cpu_set), &cpu_set);
  if (ret != 0)
    panic("err cpu affinity");
}

template <std::size_t UNROLL = 8, std::size_t CHUNK = 2 * Mb>
DBG_NOINLINE void worker_routine(const FileChunks &chunks, Result &r,
                                 std::atomic<std::size_t> &next_chunk, int idx,
                                 int cpu) {
  set_cpu_affinity(cpu);
  std::size_t cur_chunk = idx;
  const std::size_t n_chunks = chunks.size();
  while (cur_chunk < n_chunks) {
    process_batch<UNROLL, CHUNK>(r, chunks[cur_chunk]);
    cur_chunk = next_chunk.fetch_add(1, std::memory_order_relaxed);
  }
}

template <std::size_t WORKERS, std::size_t UNROLL, std::size_t UNROLL_CHUNK,
          std::size_t WORKER_CHUNK>
Result run_workers(std::span<const char> file,
                   std::span<const int, WORKERS> cpus) {
  std::vector<std::thread> workers;
  std::vector<Result> results;
  workers.reserve(WORKERS);
  results.resize(WORKERS);

  auto chunks = split_into_chunks(Chunk{file.data(), file.size()}, WORKER_CHUNK);

  alignas(64) std::atomic<std::size_t> next_chunk{WORKERS + 1};

  for (std::size_t i = 0; i < WORKERS; ++i) {
    workers.push_back(std::thread([&, i]() {
      worker_routine<UNROLL, UNROLL_CHUNK>(chunks, results[i], next_chunk, i, cpus[i]);
    }));
  }

  Result r{};
  worker_routine(chunks, r, next_chunk, WORKERS, 0);

  for (std::size_t i = 0; i < WORKERS; ++i) {
    workers[i].join();
    merge(r, results[i]);
  }
  return r;
}


static constexpr std::array<int, 13> CPUS = {1,  3,  6,  8,  10, 12, 13,
                                                  14, 15, 17, 19, 20, 21};
static constexpr std::size_t WORKERS = 13;
static_assert(WORKERS <= CPUS.size());
static constexpr std::size_t PARALLEL_CHUNK = 64 * Mb;
static constexpr std::size_t UNROLL_CHUNK = 2 * Mb;
static constexpr std::size_t UNROLL = 8;

auto run_mt(auto &&...args) {
  return run_workers<WORKERS, UNROLL, UNROLL_CHUNK, PARALLEL_CHUNK>(args...);
}

auto run_st(std::span<const char> file, Result& r) {
  Chunk c{file.data(), file.size()};
  return process_batch<UNROLL, UNROLL_CHUNK>(r, c);
};

int main() {
  static constexpr bool MULTITHREADED = true;

  MMappedFile file(DATA);
  auto file_mem =
      std::span<const char>(static_cast<const char *>(file.ptr()), file.size());

  auto stats = [&]() {
    if constexpr (MULTITHREADED) {
      return run_mt(file_mem, std::span<const int, WORKERS>(CPUS.cbegin(), WORKERS));
    } else {
      Result stats;
      run_st(file_mem, stats);
      return stats;
    }
  }();

  print_results(stats);

  return 0;
}
