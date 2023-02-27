PREFIX=/usr/local

all:
		gcc -g -O2 -Wall -pedantic -o mcdumpload mcdumpload.c $(LDFLAGS)
