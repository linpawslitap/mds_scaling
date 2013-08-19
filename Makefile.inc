#OPT ?= -O2 -DNDEBUG
OPT ?= -g2 -DDEBUG
CFLAGS	= -g -Wall -Wextra `pkg-config fuse --cflags` -Werror -D_GNU_SOURCE $(OPT) -fPIC
LDFLAGS = -lm -lpthread -lstdc++
SRCS = $(wildcard *.c)
HDRS = $(wildcard *.h) $(RPC_H)
OBJS = $(addsuffix .o, $(basename $(SRCS)))
