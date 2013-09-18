/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-09-15 14:25:33
* File Name: ./hash.h
* Description:
 ************************************************************************/

#ifndef HASH_H
#define HASH_H

#include <inttypes.h>
#include <stdlib.h>

uint32_t getStrHash(const char* data, size_t n, uint32_t seed);

#endif
