#include <algorithm>
#include <array>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

#include <sys/mman.h>
#include <sys/stat.h>

static constexpr char DATA[] = "measurements.txt";
static constexpr char DELIM = ';';
static constexpr char NEW_LINE = '\n';

struct Data {
    int64_t min = std::numeric_limits<int64_t>::max();
    int64_t max = std::numeric_limits<int64_t>::min();
    int64_t sum = 0;
    size_t count = 0;
};

using Result = std::unordered_map<std::string, Data>;

void print_results(const Result& r) {
    std::cout << std::fixed << std::setprecision(1) << "{";
    bool first = true;

    std::vector<std::pair<std::string, Data>> v;
    v.reserve(r.size());
    for (auto&& [name, data] : r) {
        v.push_back({name,data});
    }
    std::sort(v.begin(), v.end(), [](auto& a, auto& b) { return a.first < b.first; });

    for (auto&& [name, data] : v) {
        if (!first)
            std::cout << ", ";

        std::cout << name << "=" << (static_cast<double>(data.min) / 10.0) << "/"
            << (static_cast<double>(data.sum) / 10.0 / data.count) << "/"
            << (static_cast<double>(data.max) / 10.0);
        first = false;
    }
    std::cout << "}\n";
}

void panic(const char* err) {
    std::cerr << err << "\n";
    std::terminate();
}

int do_open(const char* file) {
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

void* do_mmap(int fd, size_t size) {
    void* ptr = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED)
        panic("err mmap");
    return ptr;
}

void do_unmap(void* ptr, size_t size) {
    const int ret = ::munmap(ptr, size);
    if (ret < 0) 
        panic("err munmap");
}


class MMappedFile {
public:
    MMappedFile(const char* file)
        : fd_(do_open(file))
        , size_(get_size(fd_))
        , ptr_(do_mmap(fd_, size_))
    {
        const int ret = ::madvise(ptr_, size_, MADV_SEQUENTIAL);
        if (ret < 0)
            panic("err madvise");
    }

    ~MMappedFile() {
        do_unmap(ptr_, size_);
        do_close(fd_);
    }

    void* ptr() { return ptr_; }
    size_t size() const { return size_; }
private:
    const int fd_;
    const size_t size_;
    void* ptr_;
};

void update_result(Result& r, std::string_view name, int64_t v) {
    auto& s = r[std::string(name)];
    s.min = std::min(v, s.min);
    s.max = std::max(v, s.max);
    s.sum += v;
    s.count++;
}

constexpr std::tuple<int16_t, int8_t> parse_value(const char* end) {
    std::array<int16_t, 3> digits = {0, 0, 0};
    int8_t l3 = end[-4] == DELIM;
    int8_t l4 = end[-5] == DELIM;
    int8_t l5 = (1 - l3 - l4);

    digits[2] = end[-1] - '0';
    digits[1] = end[-3] - '0';
    int16_t sign = end[-4] == '-';
    digits[0] = (1 - sign) * (1 - l3) * (end[-4] - '0');

    sign = sign | (end[-5] == '-');
    const int64_t val = (1 - 2 * sign) * (
        100 * digits[0] +
        10  * digits[1] +
        1   * digits[2]
    );
    const int8_t len = (3 * l3 + 4 * l4 + 5 * l5);
    return std::tie(val, len);
}

namespace {

static constexpr char test1[] = "a;1.2";
static constexpr char test2[] = "a;-1.2";
static constexpr char test3[] = "a;12.3";
static constexpr char test4[] = "a;-12.3";

static_assert(std::get<int16_t>(parse_value(test1 + 5)) == 12l);
static_assert(std::get<int8_t>(parse_value(test1 + 5)) == 3);
static_assert(std::get<int16_t>(parse_value(test2 + 6)) == -12);
static_assert(std::get<int8_t>(parse_value(test2 + 6)) == 4);
static_assert(std::get<int16_t>(parse_value(test3 + 6)) == 123);
static_assert(std::get<int8_t>(parse_value(test3 + 6)) == 4);
static_assert(std::get<int16_t>(parse_value(test4 + 7)) == -123);
static_assert(std::get<int8_t>(parse_value(test4 + 7)) == 5);

}


int main() {
    MMappedFile f(DATA);
    Result stats;

    size_t pos = 0;
    const char* ptr = static_cast<const char*>(f.ptr());
    const char* end = ptr + f.size();

    size_t space = std::distance(ptr, end);
    //int i = 0;
    while (/*i < 10 &&*/ space > 0) {
        const char* n = static_cast<const char*>(std::memchr(ptr, NEW_LINE, space));
        const auto [val, len] = parse_value(n);
        const auto name = std::string_view(ptr, (std::distance(ptr, n) - (len+1)));
        update_result(stats, name, val);

        ptr = (n + 1);
        space = std::distance(ptr, end);

        //std::cout << "name=" << name << " val=" << val << " len=" << len << " space=" << space << std::endl;
        //++i;
    }

    print_results(stats);
    return 0;
}
