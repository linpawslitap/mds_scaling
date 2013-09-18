#ifndef SPLIT_H
#define SPLIT_H

int get_num_split_tasks_in_progress();
int split_bucket(struct giga_directory *dir, int partition_id);

void *split_thread(void *arg);
void process_split(DIR_handle_t dir_id, index_t index);
void issue_split(DIR_handle_t *dir_id, index_t index);

#endif /* SPLIT_H */
