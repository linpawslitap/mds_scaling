include ../Makefile.inc

CFLAGS += -iquote ..

RPCGENFLAGS := -N -M

RPC_X = rpc_giga
RPC_H = ${RPC_X}.h
RPC_C = ${RPC_X}_svc.c ${RPC_X}_clnt.c ${RPC_X}_xdr.c
RPC_O = $(addsuffix .o, $(basename $(RPC_C))) rpc_helper.o

OJBS = $(RPC_O) debugging.o sha.o giga_index.o cache.o
TARGETS = ../common.a

all: $(TARGETS)

$(OBJS) : $(HDRS)

$(RPC_H) $(RPC_C) : $(RPC_X).x
	rpcgen $(RPCGENFLAGS) $(RPC_X).x
	rpcgen $(RPCGENFLAGS) -m $(RPC_X).x > $(RPC_X)_svc.c

../common.a : $(OBJS) $(RPC_O)
	ar rv ../common.a $?
	ranlib ../common.a

cache_test.o : cache_test.c cache.h
	$(CC) $(CFLAGS) -c cache_test.c

cache_test: cache_test.o ../common.a
	$(CC) -o $@ $^ $(LDFLAGS)

clean :
	rm -f $(TARGETS) $(OBJS) $(RPC_C) $(RPC_H)
