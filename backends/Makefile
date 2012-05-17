include ../Makefile.inc

CFLAGS += -iquote .. 
LDFLAGS += `pkg-config fuse --libs` ./leveldb/libleveldb.a

metadb_fs.o : metadb_fs.c operations.h
	$(CC) $(CFLAGS) -c metadb_fs.c ../common.a

rpc_fs.o : rpc_fs.c operations.h
	$(CC) $(CFLAGS) -c local_fs.c ../common.a
						
local_fs.o : local_fs.c operations.h
	$(CC) $(CFLAGS) -c local_fs.c ../common.a
						

clean :
	rm -f *.o
