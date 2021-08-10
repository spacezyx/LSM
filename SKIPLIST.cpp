#include "SKIPLIST.h"

//获取跳表大小
size_t Skiplist::size(){
    Node *tmp=head;
    uint64_t count=0;
    while(tmp->down)
        tmp=tmp->down;
    while(tmp->right){
        tmp=tmp->right;
        count++;
    }
    return count;
}


//获取value
string Skiplist::get(const uint64_t& key){
    if(!head)
        return "";
    Node *tmp=head;
    Node *t=head;

    //如果跳表为空
    if(size()==0){
        return "";
    }

    while(true){
        while(tmp->right && (tmp->right->key <= key)){
            t=tmp;
            tmp=tmp->right;
        }

        if(((tmp->key)>key)&&tmp->val!="")
            tmp=t;
        if(tmp->key==key&&tmp->val!="")
            return tmp->val;

        tmp=tmp->down;
        if(!tmp)
            return "";
    }
}

//PUT操作
//返回错误是PUT的问题
void Skiplist::put(const uint64_t& key, const string& val) {
    vector<Node*> pathList;    //从上至下记录搜索路径
    Node *p = head;
    while(p){
        //如果查找到该key 将其值替换并更新num大小
        if(p->key==key&&p->val!=""){
            num -= p->val.size();
            p->val=val;
            while(p->down){
                p=p->down;
                p->val=val;
            }
            num += p->val.size();
            return;
        }

        while(p->right && p->right->key <= key){
            p = p->right;
            //如果查找到该key
            if(p->key==key&&p->val!=""){
                num -= p->val.size();
                p->val=val;
                while(p->down){
                    p=p->down;
                    p->val=val;
                }
                num += p->val.size();
                return;
            }
        }
        pathList.push_back(p);
        p = p->down;
    }

    bool insertUp = true;
    Node* downNode= nullptr;
    num += (val.size()+12);
    while(insertUp && pathList.size() > 0){   //从下至上搜索路径回溯，50%概率
        Node *insert = pathList.back();
        pathList.pop_back();
        insert->right = new Node(insert->right, downNode, key, val); //add新结点
        //新插入的节点不仅有数据区的大小 还有索引区的大小

        downNode = insert->right;    //把新结点赋值为downNode
        insertUp = (rand()&1);   //50%概率
    }
    if(insertUp){  //插入新的头结点，加层
        Node* oldHead = head;
        head = new Node();
        head->right = new Node(NULL, downNode, key, val);
        head->down = oldHead;
    }
}


