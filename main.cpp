#include <iomanip>
#include <iostream>
#include <fstream>
#include <limits>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <algorithm>

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

int main() {
    std::ifstream f(DATA);
    std::string line;
    Result stats;

    while (std::getline(f, line, NEW_LINE)) {
        const auto pos = line.find(DELIM);
        if (pos == std::string::npos)
            break;

        const auto name = std::string_view(line.data(), pos);
        const auto val = std::atof(line.data() + pos+1);

        auto& s = stats[std::string(name)];
        s.min = std::min(val, s.min);
        s.max = std::max(val, s.max);
        s.sum += val;
        s.count++;
    }

    print_results(stats);
    return 0;
}
