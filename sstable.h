#ifndef SSTABLE_H
#define SSTABLE_H

#include <cstdint>
#include <string>
#include "MurmurHash3.h"
#include "BloomFilter.h"
#include "SKIPLIST.h"
#include <fstream>

using namespace std;

//header部分
struct Head{
public:
    uint64_t time;
    uint64_t num;
    uint64_t max;
    uint64_t min;
};

//index单元
struct Index{
    uint64_t key;
    uint32_t offset;
};

class SSTable
{
public:
    SSTable();
    //Header
    Head head;

    //bloom filter
    BloomFilter bf;

    //索引区
    vector<Index> index;

    //数据区
    string value;

    //查看是否存在某key并返回其值
    string isExist(uint64_t key);

    //二分查找key对应的offset
    vector<uint32_t> hasKey(uint64_t key);

    //生成新的SSTable
    void generateForMem(Skiplist* now,uint64_t time);

    //将SSTable写入磁盘
    void writeDisk(string path);
};

#endif // SSTABLE_H
