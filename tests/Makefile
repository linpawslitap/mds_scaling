include ../Makefile.inc
include ../backends/build_config.mk

HADOOP_LDFLAGS= -L/usr/lib/jvm/default-java/jre/lib/amd64/ \
	-L/usr/lib/jvm/default-java/jre/lib/amd64/server \
	-ljvm
HADOOP_LIB = ../hadoop/lib/native/libhdfs.a

CFLAGS += -iquote ..
LDFLAGS += $(HADOOP_LDFLAGS)
LIBS =../backends/$(LEVELDBLIBRARY) $(HADOOP_LIB) ../common.a

all:
#	gcc $(OPT) -o ./mknod_test ./mknod_test.c -lpthread
#	gcc $(OPT) -o ./mix_test ./mix_test.c -lpthread
#	gcc $(OPT) -o ./stat_test ./stat_test.c
#	gcc $(OPT) -o ./smallfile_test ./smallfile_test.c -lpthread
#	gcc $(OPT) -o ./mknod_lib_test ./mknod_lib_test.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) -I..
#	gcc $(OPT) -o ./mkdir_lib_test ./mknod_lib_test.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) -I..
	gcc $(OPT) -o ./create_all_test ./create_all_test.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) -I..
	gcc $(OPT) -o ./create_dir_test ./create_dir_test.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) -I..
	gcc $(OPT) -o ./traverse_dir_test ./traverse_dir_test.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) -I..
#	gcc $(OPT) -o ./tree_test ./tree_test.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) -I..
#	gcc $(OPT) -o ./smallfile_lib_test ./smallfile_lib_test.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) -I..
#	gcc $(OPT) -o ./ldb_hdfs_test ./ldb_hdfs_test.c  $(LIBS) $(LDFLAGS) $(HADOOP_LDFLAGS)
#	gcc $(OPT) -o ./random_test ./random_test.c  $(LIBS) $(LDFLAGS) $(HADOOP_LDFLAGS) -I..
	gcc $(OPT) -o ./tree_select_test ./tree_select_test.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) $(HADOOP_LDFLAGS) -I..
	gcc $(OPT) -o ./tree_select_test_debug ./tree_select_test_debug.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) $(HADOOP_LDFLAGS) -I..
	gcc $(OPT) -o ./tree_select_test_multi ./tree_select_test_multi.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) $(HADOOP_LDFLAGS) -I..
	gcc $(OPT) -o ./tree_debug ./tree_debug.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) $(HADOOP_LDFLAGS) -I..

clean:
	rm mknod_test stat_test smallfile_test mkdir_lib_test mknod_lib_test smallfile_lib_test ldb_hdfs_test crash_test mix_test create_all_test
