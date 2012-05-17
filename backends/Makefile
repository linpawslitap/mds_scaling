include ../Makefile.inc

CFLAGS += -iquote .. 
LDFLAGS += `pkg-config fuse --libs` ./leveldb/libleveldb.a

leveldb_backend.o : leveldb_backend.c operations.h
	$(CC) $(CFLAGS) -c leveldb_backend.c ../common.a

rpc_fs.o : rpc_fs.c operations.h
	$(CC) $(CFLAGS) -c local_fs.c ../common.a
						
local_fs.o : local_fs.c operations.h
	$(CC) $(CFLAGS) -c local_fs.c ../common.a
						

clean :
	rm -f *.o
