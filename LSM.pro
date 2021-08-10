TEMPLATE = app
CONFIG += console c++11
CONFIG -= app_bundle
CONFIG -= qt

SOURCES += \
        MurmurHash3.cpp \
        SKIPLIST.cpp \
        correctness.cpp \
        kvstore.cpp \
        sstable.cpp

HEADERS += \
    BloomFilter.h \
    MurmurHash3.h \
    SKIPLIST.h \
    kvstore.h \
    kvstore_api.h \
    sstable.h \
    test.h \
    utils.h
