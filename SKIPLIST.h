#ifndef SKIPLIST_H
#define SKIPLIST_H

//实现一个跳表
#include <vector>
#include <iostream>
#include <string>

using namespace std;

struct Node{
    Node *right,*down;   //向右向下
    uint64_t key;
    string val;
    Node(Node *right,Node *down, uint64_t key, string val): right(right), down(down), key(key), val(val){}
    Node(): right(nullptr), down(nullptr),key(0) {}
};

class Skiplist{
public:
    Node *head;
    //计算生成SSTable对应的大小
    uint64_t num;
    Skiplist(){
        head = new Node();  //初始化头结点
        num=10272;  //初始化生成对应SSTable的大小 先把Header和bloom filter的大小加上
    }

    //获取跳表大小
    size_t size();


    //获取value
    string get(const uint64_t& key);

    //PUT操作
    //返回错误是PUT的问题
    void put(const uint64_t& key, const string& val);
};


#endif // SKIPLIST_H
