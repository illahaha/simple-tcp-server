OBJECTS = main.o
CFLAGS = -std=c11 -Weverything -O2

server: $(OBJECTS)
	$(CC) $(CFLAGS) $(OBJECTS) -o server

all: server
