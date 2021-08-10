#pragma once

#include "kvstore_api.h"
#include "SKIPLIST.h"
#include "MurmurHash3.h"
#include "sstable.h"
#include <fstream>
#include <utils.h>
#include <algorithm>
#include <math.h>
#include <map>
#include <string>

class KVStore : public KVStoreAPI {
    // You can add your implementation here
private:

public:
    Skiplist* memTable=new Skiplist;
    //时间戳
    uint64_t time;

    //文件所在层
    uint64_t level;

    //数据目录
    string dirPrefix;

    //文件数目
    uint64_t fileNum;

    //新建SSTable
    SSTable* sstable=new SSTable;

    //存一下是否有level-0 以免每次都要调用函数
    bool level0flag;


    //用map存文件名-文件内容（指除value以外的部分）
    map<string,SSTable> storeData;

    //构造函数 把路径中的文件放进内存
    KVStore(const std::string &dir);

    ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void compaction();

    vector<SSTable> merge(vector<SSTable> &mergefile,bool isBottom);

    SSTable getSSTable(string fileName);

    SSTable mergeTwo(SSTable table1,SSTable table2,bool isBottom);

    vector<SSTable> divide(SSTable source);

    SSTable mergelevel(vector<SSTable> &mergefile,bool isBottom);
};
