/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-09-17 13:21:12
* File Name: ./measure.h
* Description:
 ************************************************************************/

#include <inttypes.h>

#ifndef MEASURE_H_
#define MEASURE_H_

typedef struct {
  double min_;
  double max_;
  double num_;
  double sum_;
  double sum_squares_;
} measurement_t;

uint64_t now_micros();
void start_measure();
void start_op();
void finish_op();
void finish_measure();

void measurement_clear(measurement_t *);
void measurement_add(measurement_t* hist, double value);

#endif //MEASURE_H_
