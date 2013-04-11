include ../Makefile.inc
include ../backends/build_config.mk

CFLAGS += -iquote ..
LIBS += ../backends/$(LEVELDBLIBRARY) /usr/lib/libssl.so.0.9.8 ../common.a

OBJS += ../backends/local_fs.o ../backends/rpc_fs.o ../backends/metadb_fs.o
#OBJS += ../backends/*.o

TARGETS = ../giga_server

all: $(TARGETS)

$(OBJS) : $(HDRS)

../giga_server : $(OBJS)
	$(CC) $(CFLAGS) -o $@ $^ $(LIBS) $(LDFLAGS)

clean :
	rm -f $(TARGETS) $(OBJS)
