#pragma once
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <vector>

namespace asstats {
// Offline diagnostic totals can exceed uint64_t even though each wire counter cannot.
class DecimalSum {
public:
    void add(uint64_t value) {
        uint64_t carry = 0;
        size_t i = 0;
        do {
            if (i == limbs_.size()) limbs_.push_back(0);
            const uint64_t sum = limbs_[i] + value % base + carry;
            limbs_[i++] = static_cast<uint32_t>(sum % base);
            carry = sum / base;
            value /= base;
        } while (value || carry);
    }
    std::string str() const {
        if (limbs_.empty()) return "0";
        std::ostringstream out;
        out << limbs_.back();
        for (size_t i = limbs_.size() - 1; i > 0; --i)
            out << std::setw(9) << std::setfill('0') << limbs_[i - 1];
        return out.str();
    }
private:
    static constexpr uint64_t base = 1000000000;
    std::vector<uint32_t> limbs_;
};
} // namespace asstats
