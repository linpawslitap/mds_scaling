include ../Makefile.inc

CFLAGS += -iquote ..
LDFLAGS += `pkg-config fuse --libs` ../backends/leveldb/libleveldb.a 

OBJS += ../backends/local_fs.o ../backends/rpc_fs.o ../backends/metadb_fs.o
#OBJS += ../backends/*.o

TARGETS = ../giga_client 

all: $(TARGETS)

$(OBJS) : $(HDRS)

../giga_client : $(OBJS) #../common.a
	$(CC) -o $@ $^ ../common.a $(LDFLAGS)

clean :
	rm -f $(TARGETS) $(OBJS)
