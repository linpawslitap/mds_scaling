OPT ?= -O2 -DDEBUG
# OPT ?= -g2
CFLAGS	= -g -Wall -Wextra `pkg-config fuse --cflags` -Werror -D_GNU_SOURCE $(OPT)
LDFLAGS = -lm -lpthread -lstdc++
SRCS = $(wildcard *.c)
HDRS = $(wildcard *.h) $(RPC_H)
OBJS = $(addsuffix .o, $(basename $(SRCS)))
