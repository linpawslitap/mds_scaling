include ../Makefile.inc
include ../backends/build_config.mk

HADOOP_LIB = ../hadoop/lib/native/libhdfs.a

CFLAGS += -iquote ..
LDFLAGS +=  `pkg-config fuse --libs`
LIBS = ../common.a
BACKENDOBJ = ../backends/local_fs.o ../backends/rpc_fs.o
OBJS += $(BACKENDOBJ)
LIBCLIENTOBJ = FUSE_operations.o libclient.o
TARGETS = ../libgiga_client.a ../libgiga_client.so ../giga_client

all: $(TARGETS)

$(OBJS) : $(HDRS)

libclient_test: $(BACKENDOBJ) $(LIBCLIENTOBJ) libclient_test.o
	$(CC) -o $@ $^ $(LIBS) $(LDFLAGS)

../giga_client : $(BACKENDOBJ) $(LIBCLIENTOBJ) client.o
	$(CC) -o $@ $^ $(LIBS) $(LDFLAGS)

../libgiga_client.a : $(BACKENDOBJ) $(LIBCLIENTOBJ)
	ar rv ../libgiga_client.a $?
	ranlib ../libgiga_client.a

../libgiga_client.so : $(BACKENDOBJ) $(LIBCLIENTOBJ) $(LIBS)
	$(CC) -shared -o ../libgiga_client.so $?


clean :
	rm -f $(TARGETS) $(OBJS)
