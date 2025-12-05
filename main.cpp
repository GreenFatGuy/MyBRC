#include <iomanip>
#include <iostream>
#include <fstream>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <cstring>

#include <fcntl.h>
#include <unistd.h>

#include <sys/stat.h>
#include <sys/mman.h>

static constexpr char DATA[] = "measurements.txt";
static constexpr char DELIM = ';';
static constexpr char NEW_LINE = '\n';

struct Data {
    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::min();
    double sum = 0.0;
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

        std::cout << name << "=" << data.min << "/" << (data.sum / data.count) << "/" << data.max;
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

void update_result(Result& r, std::string_view name, double v) {
    auto& s = r[std::string(name)];
    s.min = std::min(v, s.min);
    s.max = std::max(v, s.max);
    s.sum += v;
    s.count++;
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
        const char* d = static_cast<const char*>(std::memchr(ptr, DELIM, space));
        const char* n = static_cast<const char*>(std::memchr(d, NEW_LINE, space - std::distance(ptr, d)));
        const auto name = std::string_view(ptr, std::distance(ptr, d));
        const auto val = std::atof(d + 1);
        update_result(stats, name, val);

        ptr = (n + 1);
        space = std::distance(ptr, end);

        //std::cout << "name=" << name << " val=" << val << " space=" << space << "\n";
        //++i;
    }

    print_results(stats);
    return 0;
}
