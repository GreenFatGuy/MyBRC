#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string_view>
#include <tuple>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/stat.h>

static constexpr char DATA[] = "measurements.txt";
static constexpr char DELIM = ';';
static constexpr char NEW_LINE = '\n';
// static constexpr int MAX_NAMES = 10'000;

struct fnv1a {
  constexpr std::size_t operator()(const std::string &s) const noexcept {
    return fnv1a_func(s.data(), s.size());
  }

  constexpr std::size_t operator()(const std::string_view &s) const noexcept {
    return fnv1a_func(s.data(), s.size());
  }

  constexpr std::size_t operator()(const char *s) const noexcept {
    return this->operator()(std::string_view(s));
  }

private:
  constexpr std::size_t fnv1a_func(const char *s, size_t n) const noexcept {
    std::size_t h = 14695981039346656037ull;
    for (std::size_t i = 0; i < n; ++i) {
      h ^= static_cast<uint8_t>(s[i]);
      h *= 1099511628211ull;
    }
    return h;
  }
};

struct Data {
  int64_t min = std::numeric_limits<int64_t>::max();
  int64_t max = std::numeric_limits<int64_t>::min();
  int64_t sum = 0;
  size_t count = 0;
};

class Result {
public:
  static constexpr std::size_t BUCKETS = 1024;
  static constexpr std::size_t START_SIZE = 16;
  using KV = std::pair<std::string, Data>;
  using Bucket = std::vector<KV>;

  Result() = default;

  std::tuple<KV &, bool> try_emplace(std::string_view name, Data &&v) {
    const std::size_t h = fnv1a()(name);
    const std::size_t idx = h & (BUCKETS - 1);
    auto &b = map_[idx];

    auto it = std::find_if(b.begin(), b.end(),
                           [&](auto &p) { return p.first == name; });

    if (it == b.end()) [[unlikely]] {
      if (b.empty())
        b.reserve(START_SIZE);

      b.emplace_back(std::string(name), std::forward<Data>(v));
      ++size_;
      return {b.back(), true};
    } else {
      return {*it, false};
    }
  }

  std::size_t size() const noexcept { return size_; }

  std::vector<KV> to_plain() const {
    std::vector<KV> v;
    v.reserve(size());
    for (const auto &b : map_) {
      for (const auto &kv : b) {
        v.push_back(kv);
      }
    }
    std::sort(v.begin(), v.end(),
              [](auto &a, auto &b) { return a.first < b.first; });
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

    for (const auto &l : loads) {
      sum += l;
      min = std::min(l, min);
      max = std::max(l, max);
    }

    std::cerr << "MyHashMap stats load factor:\n";
    std::cerr << "\tmean: " << static_cast<double>(sum) / loads.size() << "\n";
    std::cerr << "\tmin:  " << min << "\n";
    std::cerr << "\tmax:  " << max << "\n";
  }

private:
  std::array<Bucket, BUCKETS> map_;
  std::size_t size_{0};
};

void print_results(const Result &r) {
  std::cout << std::fixed << std::setprecision(1) << "{";
  bool first = true;

  auto v = r.to_plain();
  std::sort(v.begin(), v.end(),
            [](auto &a, auto &b) { return a.first < b.first; });

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

void update_result(Result &r, std::string_view name, int64_t v) {
  auto [s, _] = r.try_emplace(name, Data{});

  s.second.max = std::max(v, s.second.max);
  s.second.min = std::min(v, s.second.min);
  s.second.sum += v;
  s.second.count++;
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

void *do_mmap(int fd, size_t size) {
  void *ptr = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
  if (ptr == MAP_FAILED)
    panic("err mmap");
  return ptr;
}

void do_unmap(void *ptr, size_t size) {
  const int ret = ::munmap(ptr, size);
  if (ret < 0)
    panic("err munmap");
}

class MMappedFile {
public:
  MMappedFile(const char *file)
      : fd_(do_open(file)), size_(get_size(fd_)), ptr_(do_mmap(fd_, size_)) {
    const int ret = ::madvise(ptr_, size_, MADV_SEQUENTIAL);
    if (ret < 0)
      panic("err madvise");
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

constexpr std::tuple<int16_t, int8_t> parse_value(const char *end) {
  std::array<int16_t, 3> digits = {0, 0, 0};
  int8_t l3 = end[-4] == DELIM;
  int8_t l4 = end[-5] == DELIM;
  int8_t l5 = (1 - l3 - l4);

  digits[2] = end[-1] - '0';
  digits[1] = end[-3] - '0';
  int16_t sign = end[-4] == '-';
  digits[0] = (1 - sign) * (1 - l3) * (end[-4] - '0');

  sign = sign | (end[-5] == '-');
  const int64_t val =
      (1 - 2 * sign) * (100 * digits[0] + 10 * digits[1] + 1 * digits[2]);
  const int8_t len = (3 * l3 + 4 * l4 + 5 * l5);
  return std::tie(val, len);
}

namespace {

static constexpr char test1[] = "a;1.2";
static_assert(std::get<int16_t>(parse_value(test1 + 5)) == 12l);
static_assert(std::get<int8_t>(parse_value(test1 + 5)) == 3);

static constexpr char test2[] = "a;-1.2";
static_assert(std::get<int16_t>(parse_value(test2 + 6)) == -12);
static_assert(std::get<int8_t>(parse_value(test2 + 6)) == 4);

static constexpr char test3[] = "a;12.3";
static_assert(std::get<int16_t>(parse_value(test3 + 6)) == 123);
static_assert(std::get<int8_t>(parse_value(test3 + 6)) == 4);

static constexpr char test4[] = "a;-12.3";
static_assert(std::get<int16_t>(parse_value(test4 + 7)) == -123);
static_assert(std::get<int8_t>(parse_value(test4 + 7)) == 5);

} // namespace

int main() {
  MMappedFile f(DATA);
  Result stats;

  const char *ptr = static_cast<const char *>(f.ptr());
  for (int64_t space = f.size(); space > 0;) {
    const char *n =
        static_cast<const char *>(std::memchr(ptr, NEW_LINE, space));
    const auto [val, len] = parse_value(n);
    const auto name =
        std::string_view(ptr, (std::distance(ptr, n) - (len + 1)));
    update_result(stats, name, val);

    space -= std::distance(ptr, n) + 1;
    ptr = (n + 1);
  }

  print_results(stats);
  return 0;
}
