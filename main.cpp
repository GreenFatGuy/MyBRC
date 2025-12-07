#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string_view>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/stat.h>

static constexpr char DATA[] = "measurements.txt";
static constexpr char DELIM = ';';
// static constexpr char NEW_LINE = '\n';
// static constexpr int MAX_NAMES = 10'000;

template <typename H>
struct HashWrapper {
  constexpr std::size_t operator()(const std::string &s) const noexcept {
    return H::hash(s.data(), s.size());
  }

  constexpr std::size_t operator()(const std::string_view &s) const noexcept {
    return H::hash(s.data(), s.size());
  }

  constexpr std::size_t operator()(const char *s) const noexcept {
    return this->operator()(std::string_view(s));
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
    for (std::size_t i = n & 7; i > 0; --i) {
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

template <std::size_t BUCKETS = 8192, std::size_t CHAIN_START_SIZE = 2, typename Hash = FNV1A>
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
    Data data;
  };

  Data &try_emplace(std::string_view name, Data &&v) {
    const std::size_t h = Hash()(name);
    std::size_t s = idx(h);
    for (std::size_t i = 0; i < BUCKETS; ++i) {
      if (map_[s].name == name) [[likely]] {
        if constexpr (DEBUG)
          hops_[i]++;

        return map_[s].data;
      }

      if (map_[s].name.empty()) [[likely]] {
        if constexpr (DEBUG)
          hops_[i]++;

        map_[s].name = std::string(name);
        map_[s].data = std::forward<Data>(v);
        ++size_;
        return map_[s].data;
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

void update_result(Result &r, std::string_view name, int16_t t) {
  auto &s = r.try_emplace(name, Data{});

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

void do_madvise(void* ptr, size_t size, int adv) {
  const int ret = ::madvise(ptr, size, adv);
  if (ret < 0)
    panic("err madvise");
}

class MMappedFile {
public:
  MMappedFile(const char *file)
      : fd_(do_open(file)), size_(get_size(fd_))
      , ptr_(do_mmap(fd_, size_, PROT_READ, MAP_PRIVATE))
  {
    do_madvise(ptr_, size_, MADV_SEQUENTIAL | MADV_HUGEPAGE);
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

constexpr temp parse_value(const char *d) {
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

int main() {
  MMappedFile f(DATA);
  void* res_ptr = do_mmap(-1, sizeof(Result), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON);
  do_madvise(res_ptr, sizeof(Result), MADV_HUGEPAGE);
  auto* stats = new (res_ptr) Result;

  const char *ptr = static_cast<const char *>(f.ptr());
  for (int64_t space = f.size(); space > 0;) {
    const char *n = static_cast<const char *>(std::memchr(ptr, DELIM, space));
    const auto [val, len] = parse_value(n + 1);
    const auto name = std::string_view(ptr, std::distance(ptr, n));
    update_result(*stats, name, val);

    space -= std::distance(ptr, n) + 1 + len + 1;
    ptr = (n + 1 + len + 1);
  }

  print_results(*stats);
  return 0;
}
