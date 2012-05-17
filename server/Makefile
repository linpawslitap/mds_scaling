include ../Makefile.inc

CFLAGS += -iquote ..
LDFLAGS += /usr/lib/libssl.so.0.9.8 ../backends/leveldb/libleveldb.a

OBJS += ../backends/local_fs.o ../backends/rpc_fs.o ../backends/metadb_fs.o
#OBJS += ../backends/*.o

TARGETS = ../giga_server

all: $(TARGETS)

$(OBJS) : $(HDRS)

../giga_server : $(OBJS) #../common.a
	$(CC) -o $@ $^ ../common.a $(LDFLAGS)

clean :
	rm -f $(TARGETS) $(OBJS)
