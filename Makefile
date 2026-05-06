CC := gcc

MODE ?= debug

SRC := lab4.c
TARGET := lab4

BUILD_DIR := build/$(MODE)

CFLAGS_common := -std=c11 -pedantic -W -Wall -Wextra

CFLAGS_debug := -O0 -g -ggdb
CFLAGS_release := -O2 -DNDEBUG

ifeq ($(MODE),debug)
    CFLAGS := $(CFLAGS_common) $(CFLAGS_debug)
else ifeq ($(MODE),release)
    CFLAGS := $(CFLAGS_common) $(CFLAGS_release)
else
    $(error MODE must be debug or release)
endif

.PHONY: all clean run debug release

all: $(BUILD_DIR)/$(TARGET)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(BUILD_DIR)/$(TARGET): $(SRC) | $(BUILD_DIR)
	$(CC) $(CFLAGS) $(SRC) -o $@

run: all
	./$(BUILD_DIR)/$(TARGET)

debug:
	$(MAKE) MODE=debug

release:
	$(MAKE) MODE=release

clean:
	rm -rf build
