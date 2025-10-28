CC = gcc
CFLAGS = -Wall -Wextra -pthread -g
SRCS = server.c
TARGET = dropbox_server

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRCS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
