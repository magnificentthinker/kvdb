CC = g++
CFLAGS = -I. -I/usr/include/gtest -I/usr/include/gtest/internal
LDFLAGS = -lgtest -lgtest_main -lpthread

TEST ?=

ifeq ($(TEST),SkiplistTest)
SRC = db/skiplist_test.cc
endif
ifeq ($(TEST),LRUTest)
SRC = util/LRU_cache_test.cc
endif
ifeq ($(TEST),HashTest)
SRC = util/hash_test.cc
endif
ifeq ($(TEST),CacheTest)
SRC = util/cache_test.cc
endif

TARGET = build/output

all: $(TARGET)

$(TARGET): $(SRC)
	@echo "Building $@..."
	@mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -o $(TARGET) $(SRC) $(LDFLAGS)

clean:
	rm -rf build


run: $(TARGET)
	@echo "Running..."
	./$(TARGET)