/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-08-23 14:17:23
* File Name: ./bytes_test.cc
* Description:
 ************************************************************************/
#include <stdio.h>
#include "bytes_test.h"

int getBytes(char buf[]) {
  int i;
  for (i = 0; i < 1024; ++i) {
    buf[i] = 'a'+(i%26);
    printf("%c", buf[i]);
  }
  printf("\n");
  return 1024;
}
/*
int main() {
  char buf[1024];
  getBytes(buf);
  return 0;
}
*/
