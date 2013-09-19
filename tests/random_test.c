/*************************************************************************
* Author: Kai Ren
* Created Time: 2013-09-15 14:44:23
* File Name: ./random_test.c
* Description:
 ************************************************************************/

#include "common/hash.h"
#include "common/sha.h"
#include <string.h>
#include <stdio.h>

int main() {
  FILE* file = fopen("1.txt", "r");
  if (file == NULL)
    return 0;
  char path[1024];
  int inode_num, server_id;
  uint8_t hash[SHA1_HASH_SIZE];
  while (fscanf(file, "%s %d %d", path, &inode_num, &server_id)>0) {
    char* pos = strrchr(path, '/')+1;
    shahash(pos, strlen(pos), hash);
    printf("%s %d\n", pos, hash[SHA1_HASH_SIZE-1]);
  }
  fclose(file);
  return 0;
}
