#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include "MurmurHash3.h"
#include <iostream>
#include <vector>

using namespace std;


//BloomFilter
class BloomFilter {
public:
    //初始化bitmap的大小
    bool bitmap[10240]={false};
    BloomFilter(){}
    //加入新的值
    void add(uint64_t key){
        unsigned int hash[4] = {0};
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        for(int i=0;i<4;i++)
            bitmap[hash[i]%(10240)]=true;
    }
    //查看某值是否有可能出现
    bool possiblyContains(uint64_t key){
        unsigned int hash[4] = {0};
        MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
        for(int i=0;i<4;i++)
            if(bitmap[hash[i]%(10240)]==false)
                return false;
        return true;
    }
    //清空BloomFilter
    void clear(){
      for(int i=0;i<10240;i++)
        bitmap[i]=false;
    }
};
#endif // BLOOMFILTER_H

