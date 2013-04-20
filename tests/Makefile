include ../Makefile.inc
include ../backends/build_config.mk

CFLAGS += -iquote ..
LIBS =../backends/$(LEVELDBLIBRARY) ../common.a

all:
	gcc -o ./mknod_test ./mknod_test.c -lpthread
	gcc -o ./stat_test ./stat_test.c
	gcc -o ./smallfile_test ./smallfile_test.c -lpthread
	gcc -o ./mknod_lib_test ./mknod_lib_test.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) -I..
	gcc -o ./smallfile_lib_test ./smallfile_lib_test.c -I.. ../libgiga_client.a $(LIBS) $(LDFLAGS) -I..


clean:
	rm mknod_test stat_test smallfile_test mknod_lib_test smallfile_lib_test
