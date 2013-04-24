include ../Makefile.inc
include ../backends/build_config.mk

CFLAGS += -iquote ..
LDFLAGS +=  `pkg-config fuse --libs`
LIBS =../backends/$(LEVELDBLIBRARY) ../common.a
BACKENDOBJ = ../backends/local_fs.o ../backends/rpc_fs.o ../backends/metadb_fs.o
OBJS += $(BACKENDOBJ)
LIBCLIENTOBJ = FUSE_operations.o libclient.o
TARGETS = ../giga_client ../libgiga_client.a

all: $(TARGETS)

$(OBJS) : $(HDRS)

../giga_client : $(OBJS)
	$(CC) -o $@ $^ $(LIBS) $(LDFLAGS)

../libgiga_client.a : $(BACKENDOBJ) $(LIBCLIENTOBJ)
	ar rv ../libgiga_client.a $?
	ranlib ../libgiga_client.a

clean :
	rm -f $(TARGETS) $(OBJS)
