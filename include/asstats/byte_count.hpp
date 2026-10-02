#pragma once
#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
namespace asstats {
__extension__ typedef unsigned __int128 UInt128;
class ByteCount {
public:
    explicit ByteCount(uint64_t n=0):value_(n) {}
    static ByteCount maximum() {ByteCount n;n.value_=~UInt128(0);return n;}
    ByteCount multiplied(uint64_t factor) const {
        if(factor&&value_>~UInt128(0)/factor) throw std::overflow_error("128-bit byte multiplication overflow");
        ByteCount n;n.value_=value_*factor;return n;
    }
    void add(ByteCount n) {
        if(n.value_>~UInt128(0)-value_) throw std::overflow_error("128-bit byte sum overflow");
        value_+=n.value_;
    }
    std::string str() const {
        auto n=value_;std::string s;
        do {s+=static_cast<char>('0'+n%10);n/=10;} while(n);
        std::reverse(s.begin(),s.end());return s;
    }
    bool operator==(const ByteCount&) const = default;
private:
    UInt128 value_;
};
} // namespace asstats
