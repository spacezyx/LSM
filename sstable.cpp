#include "sstable.h"

SSTable::SSTable()
{
  //index=new Index[head.num];
  value="";
  head.max=0;
  head.min=0;
  head.num=0;
  head.time=0;
}

vector<uint32_t> SSTable::hasKey(uint64_t key){
  vector<uint32_t> result;
  int low = 0;
  int high = head.num - 1;
  int mid = (low + high) / 2;
  while (low <= high){
      if (index[mid].key == key){
          if(index.size()>mid+1){
              result.push_back(index[mid].offset);
              result.push_back(index[mid+1].offset);
              return result;
            }
          else{
              result.push_back(index[mid].offset);
              return result;
            }

        }
      else if (key < index[mid].key)
        high = mid-1;
      else
        low = mid + 1;
      mid= (low + high) / 2;
    }
  return result;
}

void SSTable::generateForMem(Skiplist* now,uint64_t time){
  //清空
  value="";
  head.max=0;
  head.min=0;
  head.num=0;
  head.time=0;
  bf.clear();
  index.clear();

  Node *p = now->head;
  Node *tmp = now->head;
  uint64_t size=0;
//  uint64_t count=0;
  while(p->down)
    p=p->down;
  head.time=time;
  head.min=p->right->key;
  tmp=p;
  while(tmp->right){
      Node t=*(tmp->right);
      size++;
      Index nowValue;
      nowValue.key=t.key;
      nowValue.offset=value.size();
      value += t.val;
      bf.add(t.key);
      index.push_back(nowValue);
      tmp=tmp->right;
    }
  head.max=tmp->key;
  head.num=size;
}

void SSTable::writeDisk(string path){
  ofstream outFile(path, std::ios::binary);
  int size1=sizeof(head);
  int size2=sizeof(bf);
  int size3=sizeof(index[0].key);
  int size4=sizeof(index[0].offset);
  int size5=sizeof(value[0]);

  outFile.write(reinterpret_cast<char*>(&(head)), size1);
  outFile.write(reinterpret_cast<char*>(&(bf)), size2);
  uint64_t si2=index.size();

  for(uint64_t i=0;i<si2;i++){
      outFile.write(reinterpret_cast<char*>(&(index[i].key)), size3);
      outFile.write(reinterpret_cast<char*>(&(index[i].offset)), size4);
    }
  uint64_t si3=value.size();
  for(uint64_t i=0;i<si3;i++)
    outFile.write(reinterpret_cast<char*>(&(value[i])), size5);
  outFile.close();
  return;
}


