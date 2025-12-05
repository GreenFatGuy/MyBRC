#include <iomanip>
#include <iostream>
#include <fstream>
#include <limits>
#include <string_view>
#include <map>

static constexpr char DATA[] = "measurements.txt";
static constexpr char DELIM = ';';
static constexpr char NEW_LINE = '\n';

struct Data {
    double min = std::numeric_limits<double>::max();
    double max = std::numeric_limits<double>::min();
    double sum = 0.0;
    size_t count = 0;
};

using Result = std::map<std::string, Data>;

void print_results(const Result& r) {
    std::cout << std::fixed << std::setprecision(1) << "{";
    bool first = true;
    for (auto&& [name, data] : r) {
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
