CC = gcc
CFLAGS = -Wall -Wextra -g
SRCS = simple_client.c
TARGET = simple_client

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRCS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
