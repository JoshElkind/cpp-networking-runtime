#pragma once

#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace nmr {

// Fixed-size log-linear histogram, in the spirit of HdrHistogram.
//
// Values below 2^kSubBucketBits are recorded exactly. Above that, each power
// of two is split into 2^(kSubBucketBits-1) linear buckets, which bounds the
// relative error at 1 / 2^(kSubBucketBits-1) (~3.1% with the default) across
// the whole uint64 range. Recording is a couple of shifts and an increment,
// with no allocation, so it can sit on a hot path.
class LatencyHistogram {
 public:
  // TODO: make sub-bucket precision a template param?
  static constexpr unsigned kSubBucketBits = 6;
  static constexpr std::uint64_t kSubBuckets = std::uint64_t{1} << kSubBucketBits;
  static constexpr std::uint64_t kHalf = kSubBuckets / 2;
  static constexpr std::size_t kBuckets = (64 - kSubBucketBits) * kHalf + kSubBuckets;
  static constexpr double kMaxRelativeError = 1.0 / static_cast<double>(kHalf);

  void record(std::uint64_t value) noexcept { record_n(value, 1); }

  void record_n(std::uint64_t value, std::uint64_t n) noexcept {
    counts_[bucket_index(value)] += n;
    count_ += n;
    sum_ += static_cast<double>(value) * static_cast<double>(n);
    if (value < min_) min_ = value;
    if (value > max_) max_ = value;
  }

  void merge(const LatencyHistogram& other) noexcept {
    for (std::size_t i = 0; i < kBuckets; ++i) counts_[i] += other.counts_[i];
    count_ += other.count_;
    sum_ += other.sum_;
    if (other.min_ < min_) min_ = other.min_;
    if (other.max_ > max_) max_ = other.max_;
  }

  void reset() noexcept { *this = LatencyHistogram{}; }

  std::uint64_t count() const noexcept { return count_; }
  std::uint64_t min() const noexcept { return count_ == 0 ? 0 : min_; }
  std::uint64_t max() const noexcept { return max_; }
  double mean() const noexcept { return count_ == 0 ? 0.0 : sum_ / static_cast<double>(count_); }

  // Smallest value v such that at least p% of samples are <= v (to within
  // bucket resolution; reports the bucket's upper edge, clamped to max()).
  std::uint64_t percentile(double p) const noexcept {
    if (count_ == 0) return 0;
    if (p <= 0.0) return min();
    auto target = static_cast<std::uint64_t>(std::ceil(p / 100.0 * static_cast<double>(count_)));
    if (target == 0) target = 1;
    if (target > count_) target = count_;

    std::uint64_t seen = 0;
    for (std::size_t i = 0; i < kBuckets; ++i) {
      seen += counts_[i];
      if (seen >= target) return bucket_upper(i) < max_ ? bucket_upper(i) : max_;
    }
    return max_;
  }

  static constexpr std::size_t bucket_index(std::uint64_t v) noexcept {
    if (v < kSubBuckets) return static_cast<std::size_t>(v);
    const unsigned shift = static_cast<unsigned>(std::bit_width(v)) - kSubBucketBits;
    return static_cast<std::size_t>(shift * kHalf + (v >> shift));
  }

  static constexpr std::uint64_t bucket_lower(std::size_t idx) noexcept {
    if (idx < kSubBuckets) return idx;
    const std::uint64_t shift = idx / kHalf - 1;
    return (idx - shift * kHalf) << shift;
  }

  static constexpr std::uint64_t bucket_upper(std::size_t idx) noexcept {
    if (idx < kSubBuckets) return idx;
    const std::uint64_t shift = idx / kHalf - 1;
    return bucket_lower(idx) + ((std::uint64_t{1} << shift) - 1);
  }

 private:
  std::array<std::uint64_t, kBuckets> counts_{};
  std::uint64_t count_ = 0;
  std::uint64_t min_ = std::numeric_limits<std::uint64_t>::max();
  std::uint64_t max_ = 0;
  double sum_ = 0.0;
};

static_assert(LatencyHistogram::bucket_index(std::numeric_limits<std::uint64_t>::max()) ==
              LatencyHistogram::kBuckets - 1);
static_assert(LatencyHistogram::bucket_lower(LatencyHistogram::bucket_index(1000)) <= 1000);
static_assert(LatencyHistogram::bucket_upper(LatencyHistogram::bucket_index(1000)) >= 1000);

}  // namespace nmr
