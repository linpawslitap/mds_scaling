#ifndef SPLIT_H
#define SPLIT_H   

//#include "common/cache.h"

//#include "backends/operations.h"

//int split_bucket(struct MetaDB ldb_mds,
int split_bucket(struct giga_directory *dir, int partition_id);

void *split_thread(void *arg);
void process_split(DIR_handle_t dir_id, index_t index);
void issue_split(DIR_handle_t *dir_id, index_t index);

#endif /* SPLIT_H */
