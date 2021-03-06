/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-09-17 12:53:12
* File Name: measure.c
* Description:
 ************************************************************************/
#include <time.h>
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <memory.h>
#include "measure.h"

histogram_t hist;
uint64_t start_time;
uint64_t finish_time;

static const double kBucketLimit[kNumBuckets] = {
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 45,
  50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200, 250, 300, 350, 400, 450,
  500, 600, 700, 800, 900, 1000, 1200, 1400, 1600, 1800, 2000, 2500, 3000,
  3500, 4000, 4500, 5000, 6000, 7000, 8000, 9000, 10000, 12000, 14000,
  16000, 18000, 20000, 25000, 30000, 35000, 40000, 45000, 50000, 60000,
  70000, 80000, 90000, 100000, 120000, 140000, 160000, 180000, 200000,
  250000, 300000, 350000, 400000, 450000, 500000, 600000, 700000, 800000,
  900000, 1000000, 1200000, 1400000, 1600000, 1800000, 2000000, 2500000,
  3000000, 3500000, 4000000, 4500000, 5000000, 6000000, 7000000, 8000000,
  9000000, 10000000, 12000000, 14000000, 16000000, 18000000, 20000000,
  25000000, 30000000, 35000000, 40000000, 45000000, 50000000, 60000000,
  70000000, 80000000, 90000000, 100000000, 120000000, 140000000, 160000000,
  180000000, 200000000, 250000000, 300000000, 350000000, 400000000,
  450000000, 500000000, 600000000, 700000000, 800000000, 900000000,
  1000000000, 1200000000, 1400000000, 1600000000, 1800000000, 2000000000,
  2500000000.0, 3000000000.0, 3500000000.0, 4000000000.0, 4500000000.0,
  5000000000.0, 6000000000.0, 7000000000.0, 8000000000.0, 9000000000.0,
  1e200,
};

void histogram_clear(histogram_t *hist) {
  hist->min_ = kBucketLimit[kNumBuckets-1];
  hist->max_ = 0;
  hist->num_ = 0;
  hist->sum_ = 0;
  hist->sum_squares_ = 0;
  int i;
  for (i = 0; i < kNumBuckets; i++) {
    hist->buckets_[i] = 0;
  }
}

void histogram_add(histogram_t* hist, double value) {
  // Linear search is fast enough for our usage in db_bench
  int b = 0;
  while (b < kNumBuckets - 1 && kBucketLimit[b] <= value) {
    b++;
  }
  hist->buckets_[b] += 1.0;
  if (hist->min_ > value) hist->min_ = value;
  if (hist->max_ < value) hist->max_ = value;
  hist->num_++;
  hist->sum_ += value;
  hist->sum_squares_ += (value * value);
}

void histogram_merge(histogram_t* hist, histogram_t* other) {
  if (other->min_ < hist->min_) hist->min_ = other->min_;
  if (other->max_ > hist->max_) hist->max_ = other->max_;
  hist->num_ += other->num_;
  hist->sum_ += other->sum_;
  hist->sum_squares_ += other->sum_squares_;
  int b;
  for (b = 0; b < kNumBuckets; b++) {
    hist->buckets_[b] += other->buckets_[b];
  }
}

double histogram_percentile(histogram_t* hist, double p) {
  double threshold = hist->num_ * (p / 100.0);
  double sum = 0;
  int b;
  for (b = 0; b < kNumBuckets; b++) {
    sum += hist->buckets_[b];
    if (sum >= threshold) {
      // Scale linearly within this bucket
      double left_point = (b == 0) ? 0 : kBucketLimit[b-1];
      double right_point = kBucketLimit[b];
      double left_sum = sum - hist->buckets_[b];
      double right_sum = sum;
      double pos = (threshold - left_sum) / (right_sum - left_sum);
      double r = left_point + (right_point - left_point) * pos;
      if (r < hist->min_) r = hist->min_;
      if (r > hist->max_) r = hist->max_;
      return r;
    }
  }
  return hist->max_;
}

double histogram_median(histogram_t* hist) {
  return histogram_percentile(hist, 50.0);
}

double histogram_average(histogram_t* hist) {
  if (hist->num_ == 0.0) return 0;
  return hist->sum_ / hist->num_;
}

double histogram_std(histogram_t *hist) {
  if (hist->num_ == 0.0) return 0;
  double variance = (hist->sum_squares_ * hist->num_ - hist->sum_ * hist->sum_) / (hist->num_ * hist->num_);
  return sqrt(variance);
}

void histogram_print(histogram_t *hist) {
  printf("Count: %.0f  Average: %.4f  StdDev: %.2f\n",
          hist->num_, histogram_average(hist), histogram_std(hist));
  printf("Min: %.4f  Median: %.4f  Max: %.4f\n",
         (hist->num_ == 0.0 ? 0.0 : hist->min_), histogram_median(hist),
         hist->max_);
  printf("------------------------------------------------------\n");
  double mult = 100.0 / hist->num_;
  double sum = 0;
  int b;
  for (b = 0; b < kNumBuckets; b++) {
    if (hist->buckets_[b] <= 0.0) continue;
    sum += hist->buckets_[b];
    printf("[ %7.0f, %7.0f ) %7.0f %7.3f%% %7.3f%% ",
           ((b == 0) ? 0.0 : kBucketLimit[b-1]),      // left
           kBucketLimit[b],                           // right
           hist->buckets_[b],                               // count
           mult * hist->buckets_[b],                        // percentage
           mult * sum);                               // cumulative percentage

    // Add hash marks based on percentage; 20 marks for 100%.
    int marks = (int)(20*(hist->buckets_[b] / hist->num_) + 0.5);
    int c;
    for (c=0; c<marks; ++c)
      printf("#");
    printf("\n");
  }
}

uint64_t now_micros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (uint64_t) tv.tv_sec * 1000000 + tv.tv_usec;
}

void start_measure() {
    memset(&hist, 0, sizeof(hist));
}

void start_op() {
    start_time = now_micros();
}

void finish_op() {
    finish_time = now_micros();
    histogram_add(&hist, finish_time - start_time);
}

void finish_measure() {
    histogram_print(&hist);
}

void measurement_clear(measurement_t *hist) {
  hist->min_ = 1000000000;
  hist->max_ = 0;
  hist->num_ = 0;
  hist->sum_ = 0;
  hist->sum_squares_ = 0;
}

void measurement_add(measurement_t* hist, double value) {
  // Linear search is fast enough for our usage in db_bench
  if (hist->min_ > value) hist->min_ = value;
  if (hist->max_ < value) hist->max_ = value;
  hist->num_++;
  hist->sum_ += value;
  hist->sum_squares_ += (value * value);
}
