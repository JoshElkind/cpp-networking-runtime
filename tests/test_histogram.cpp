#include "test.hpp"

#include "nmr/histogram.hpp"

#include <cmath>
#include <cstdint>
#include <random>

using nmr::LatencyHistogram;

TEST(histogram_small_values_exact) {
  LatencyHistogram h;
  for (std::uint64_t v = 0; v < LatencyHistogram::kSubBuckets; ++v) h.record(v);
  CHECK_EQ(h.count(), LatencyHistogram::kSubBuckets);
  CHECK_EQ(h.min(), 0u);
  CHECK_EQ(h.max(), LatencyHistogram::kSubBuckets - 1);
  CHECK_EQ(h.percentile(50), 31u);
  CHECK_EQ(h.percentile(100), 63u);
}

TEST(histogram_buckets_are_contiguous) {
  for (std::size_t i = 1; i < LatencyHistogram::kBuckets; ++i) {
    REQUIRE(LatencyHistogram::bucket_lower(i) == LatencyHistogram::bucket_upper(i - 1) + 1);
  }
  CHECK_EQ(LatencyHistogram::bucket_upper(LatencyHistogram::kBuckets - 1), UINT64_MAX);
}

TEST(histogram_index_roundtrip_within_error_bound) {
  std::mt19937_64 rng(7);
  for (int i = 0; i < 100'000; ++i) {
    const std::uint64_t v = rng() >> (rng() % 64);
    const std::size_t idx = LatencyHistogram::bucket_index(v);
    REQUIRE(LatencyHistogram::bucket_lower(idx) <= v);
    REQUIRE(v <= LatencyHistogram::bucket_upper(idx));
    const double width =
        static_cast<double>(LatencyHistogram::bucket_upper(idx) - LatencyHistogram::bucket_lower(idx));
    REQUIRE(width <= LatencyHistogram::kMaxRelativeError * static_cast<double>(v) + 1.0);
  }
}

TEST(histogram_percentiles_of_uniform) {
  LatencyHistogram h;
  for (std::uint64_t v = 1; v <= 100'000; ++v) h.record(v);
  const auto near = [](std::uint64_t got, double want) {
    return std::abs(static_cast<double>(got) - want) <= want * LatencyHistogram::kMaxRelativeError;
  };
  CHECK(near(h.percentile(50), 50'000));
  CHECK(near(h.percentile(99), 99'000));
  CHECK(near(h.percentile(99.9), 99'900));
  CHECK_EQ(h.percentile(100), 100'000u);
  CHECK(std::abs(h.mean() - 50'000.5) < 1e-6);
}

TEST(histogram_merge) {
  LatencyHistogram a;
  LatencyHistogram b;
  a.record(10);
  b.record(1'000'000);
  b.record_n(500, 3);
  a.merge(b);
  CHECK_EQ(a.count(), 5u);
  CHECK_EQ(a.min(), 10u);
  CHECK_EQ(a.max(), 1'000'000u);
}

TEST(histogram_empty) {
  LatencyHistogram h;
  CHECK_EQ(h.count(), 0u);
  CHECK_EQ(h.percentile(99), 0u);
  CHECK_EQ(h.min(), 0u);
  CHECK(h.mean() == 0.0);
}
