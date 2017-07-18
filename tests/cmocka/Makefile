PG_CONFIG = pg_config
TOP_SRC_DIR = ../../src

CC = gcc
CFLAGS += -I $(TOP_SRC_DIR) -I $(shell $(PG_CONFIG) --includedir-server)
CFLAGS += -I$(CURDIR)/../../src/include -I.
CFLAGS += $(shell $(PG_CONFIG) --cflags_sl)
CFLAGS += $(shell $(PG_CONFIG) --cflags)
CFLAGS += $(CFLAGS_SL)
CFLAGS += $(PG_CPPFLAGS)
LDFLAGS += -lcmocka
TEST_BIN = rangeset_tests

OBJ = missing_basic.o missing_list.o missing_stringinfo.o \
	  missing_bitmapset.o rangeset_tests.o $(TOP_SRC_DIR)/rangeset.o


all: build_extension $(TEST_BIN)

$(TEST_BIN): $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)

%.o: %.c
	$(CC) -c -o $@ $< $(CFLAGS)

build_extension:
	$(MAKE) -C $(TOP_SRC_DIR)/..

clean:
	rm -f $(OBJ) $(TEST_BIN)

check: all
	./$(TEST_BIN)
