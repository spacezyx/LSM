#include "kvstore.h"

//构造函数 把路径中的文件相关部分加入内存
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
  time=0;
  level=1;
  dirPrefix=dir;
  //获取已有内容
  if(utils::dirExists(dirPrefix+"/level-0")){
      vector<string> dirlist;
      utils::scanDir(dirPrefix,dirlist);
      int dirsize=dirlist.size();
      for(int i=0;i<dirsize;i++){
          vector<string> filelist;
          utils::scanDir(dirPrefix+"/"+dirlist[i],filelist);
          int filesize=filelist.size();
          for(int j=0;j<filesize;j++){
              SSTable tmp;
              string fileName=dirPrefix+"/"+dirlist[i]+"/"+filelist[j];
              std::ifstream istrm(fileName, std::ios::binary);
              if (!istrm.is_open()) {
                  std::cout << "failed to open "<<fileName<< '\n';
                } else {
                  istrm.read(reinterpret_cast<char*>(&(tmp.head)), 32);
                  istrm.read(reinterpret_cast<char*>(&(tmp.bf)), 10240);
                  uint64_t si2=tmp.head.num;
                  for(uint64_t i=0;i<si2;i++){
                      Index tmpindex;
                      istrm.read(reinterpret_cast<char*>(&(tmpindex.key)), 8);
                      istrm.read(reinterpret_cast<char*>(&(tmpindex.offset)), 4);
                      tmp.index.push_back(tmpindex);
                    }
                }
              storeData.insert(pair<string,SSTable>(fileName,tmp));
            }
        }
      level0flag=true;
    }
}

KVStore::~KVStore()
{
  //程序正常退出时把memTable转换成SSTable写入磁盘
  time++;
  sstable->generateForMem(memTable,time);
  delete memTable;
  string dirName=dirPrefix+"/level-0";
  string fileName=dirName+"/"+std::to_string(time)+".sst";
  sstable->writeDisk(fileName);
  sstable->value="";
  storeData.insert(pair<string,SSTable>(fileName,*sstable));
  std::vector<string> ret;
  utils::scanDir(dirName,ret);
  //如果level-0中的文件数超过2 触发compaction
  if(ret.size()>2)
    compaction();
  return;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
  //放不下了要生成SSTable 并把time+1
  if((memTable->num+(uint64_t)s.size()+(uint64_t)12)<=(2097152)){
      memTable->put(key,s);
      return;
    }
  else{
      time++;
      sstable->generateForMem(memTable,time);
      //如果不存在level-0 新建level-0
      if(!level0flag)
        utils::mkdir((dirPrefix+"/level-0").c_str());
      //生成SSTable并将最新的input存入新memTable
      delete memTable;
      memTable=new Skiplist;
      memTable->put(key,s);
      string dirName=dirPrefix+"/level-0";

      string fileName=dirName+"/"+std::to_string(time)+".sst";
      sstable->writeDisk(fileName);
      sstable->value="";
      storeData.insert(pair<string,SSTable>(fileName,*sstable));
      std::vector<string> ret;
      utils::scanDir(dirName,ret);
      //如果level-0中的文件数超过2 触发compaction
      if(ret.size()>2)
        compaction();
      return;
    }
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
  //先在memTable中查找
  string result=memTable->get(key);
  if(result=="~DELETED~")
    return "";
  //如果没找到在SSTable文件中查找
  else if(result==""){
      int size=storeData.size();
      if(size==0)
        return "";
      //越靠上的文件越新
      auto it=storeData.begin();
      for(;it!=storeData.end();it++){
          SSTable tmpstoredata=it->second;
          string fileName=it->first;
          //查看文件数据范围 并利用BloomFilter判断数据是否有可能存在
          if(tmpstoredata.head.max>=key&&tmpstoredata.head.min<=key){
              bool flag=tmpstoredata.bf.possiblyContains(key);
              if(flag){
                  vector<uint32_t> tmpoff=tmpstoredata.hasKey(key);
                  if(tmpoff.size()==0)
                    continue;
                  else{
                      std::ifstream istrm(fileName, std::ios::binary);
                      if (!istrm.is_open()) {
                          std::cout << "failed to open "<<fileName<< '\n';
                          return "";
                        } else {
                          uint64_t stringlen;
                          //如果不是最后一个index 则有两个元素 stringlength=offset之差
                          if(tmpoff.size()==2){
                              stringlen=tmpoff[1]-tmpoff[0];
                              if(stringlen==0)
                                stringlen=uint64_t((uint64_t)1<<32);
                            }
                          //否则则为最后一个元素 取offset位置到文件尾
                          else{
                              stringlen=uint64_t((uint64_t)1<<32);
                            }

                          SSTable sstmp=tmpstoredata;
                          uint64_t off=tmpoff[0]+32+10240+12*sstmp.head.num;
                          istrm.seekg(off);
                          for(uint64_t i=0;(i<stringlen)&&!(istrm.eof());i++){
                              char in;
                              istrm.read(reinterpret_cast<char*>(&in), sizeof (in) );
                              result += in;
                            }
                          //取到文件尾pop掉\000
                          if(istrm.eof()){
                              result.pop_back();
                            }
                          //如果发现value为~DELETED~ 则此内容已被删除 返回空
                          if(result=="~DELETED~")
                            return "";
                          return result;
                        }
                    }
                }
            }
        }
      //如果没找到 返回空
      return "";
    }
  else
    return result;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
  //memTable里说这个key已被删除
  string tmp=memTable->get(key);
  if(tmp=="~DELETED~"){
      put(key,"~DELETED~");
      return false;
    }
  //memTable里有这个key
  else if(tmp!=""){
      put(key,"~DELETED~");
      return true;
    }
  //memTable没有这个key的消息
  else{
      //SSTable里发现它被删除了
      string t=get(key);
      if(t==""||t=="~DELETED~"){
          put(key,"~DELETED~");
          return false;
        }
      //SSTable里发现了这个key
      else {
          put(key,"~DELETED~");
          return true;
        }
    }
  put(key,"~DELETED~");
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
  //清空路径中文件
  vector<string> dir;
  utils::scanDir(dirPrefix,dir);
  if(dir.size()==0)
    return;
  auto it=storeData.begin();
  for(;it!=storeData.end();){
      string fileName=it->first;
      utils::rmfile(fileName.c_str());
      auto tmp=it;
      it++;
      storeData.erase(tmp);
    }
  int dirsize=dir.size();
  for(int i=0;i<dirsize;i++){
      utils::rmdir((dirPrefix+"/"+dir[i]).c_str());
    }

  delete memTable;
  delete sstable;
  memTable=new Skiplist;
  sstable=new SSTable;
  level0flag=false;
}

//用于排序 确保更新的文件在map的前面
bool comp(const string &a,const string &b){
  int sizea=a.size();
  int sizeb=b.size();
  if(sizea<sizeb)
    return true;
  else if(sizea>sizeb)
    return false;
  else if(sizea==sizeb){
      if((a.find("level-0")!=string::npos)&&(b.find("level-0")!=string::npos))
         return (a>b);
      else
        return (a<b);
    }
}

//这里写的不太好 代码重复率有点高
void KVStore::compaction(){
  std::vector<string> level0;
  utils::scanDir(dirPrefix+"/level-0",level0);
  vector<SSTable> mergefile;
  int filesize=level0.size();
  //获取需要归并的内容
  for(int i=0;i<filesize;i++){
      string fileName=dirPrefix+"/level-0/"+level0[i];
      SSTable tmp=getSSTable(fileName);
      storeData.erase(fileName);
      if(utils::rmfile(fileName.c_str())){
          cout<<"failed to delete "<<fileName<<endl;
        }
      mergefile.push_back(tmp);
    }

  //第一次level-1还不存在文件时 直接将level-0的三个文件归并放入level-1
  if(!utils::dirExists(dirPrefix+"/level-1")){
      utils::mkdir((dirPrefix+"/level-1").c_str());
      //开始归并
      vector<SSTable>result=merge(mergefile,true);
      int resultsize=result.size();
      uint64_t resulttime=mergefile[0].head.time;
      //生成这次所有生成的header里面的time
      for(int i=1;i<filesize;i++){
          if(mergefile[i].head.time>resulttime)
            resulttime=mergefile[i].head.time;
        }
      for(int i=0;i<resultsize;i++){
          result[i].head.time=resulttime;
          string path=dirPrefix+"/level-1/"+std::to_string(resulttime)+"-"+std::to_string(i)+".sst";
          result[i].writeDisk(path);
          result[i].value="";
          storeData.insert(pair<string,SSTable>(path,result[i]));
        }
      level++;
    }
  //之后的每一次都要进行合并文件操作
  else{
      uint64_t level0range=mergefile.size();
      uint64_t min0=mergefile[0].head.min;
      uint64_t max0=mergefile[0].head.max;
      for(uint64_t i=1;i<level0range;i++){
          if(mergefile[i].head.min<min0)
            min0=mergefile[i].head.min;
          if(mergefile[i].head.max>max0)
            max0=mergefile[i].head.max;
        }
      std::vector<string> level1;
      utils::scanDir(dirPrefix+"/level-1",level1);
      uint64_t level1size=level1.size();

      //找到所有应该归并的内容
      for(uint64_t i=0;i<level1size;i++){
          level1[i]=dirPrefix+"/level-1/"+level1[i];
          if((((storeData[level1[i]].head.min)<=max0)&&((storeData[level1[i]].head.min)>=min0))
             ||(((storeData[level1[i]].head.max)<=max0)&&((storeData[level1[i]].head.max)>=min0))){
              SSTable tmp=getSSTable(level1[i]);
              storeData.erase(level1[i]);
              if(utils::rmfile(level1[i].c_str())){
                  cout<<"failed to delete "<<level1[i]<<endl;
                }
              mergefile.push_back(tmp);
            }
        }
      //开始归并
      vector<SSTable> result;
      if(!utils::dirExists(dirPrefix+"/level-2"))
        result=merge(mergefile,true);
      else
        result=merge(mergefile,false);
      int resultsize=result.size();
      uint64_t resulttime=mergefile[0].head.time;
      //生成这次所有生成的header里面的time
      for(int i=1;i<filesize;i++){
          if(mergefile[i].head.time>resulttime)
            resulttime=mergefile[i].head.time;
        }
      for(int i=0;i<resultsize;i++){
          result[i].head.time=resulttime;
          string path=dirPrefix+"/level-1/"+std::to_string(resulttime)+"-"+std::to_string(i)+".sst";
          result[i].writeDisk(path);
          result[i].value="";
          storeData.insert(pair<string,SSTable>(path,result[i]));
        }
    }

  //慢慢归并level-1及以下
  uint64_t nowlevel=1;
  while(nowlevel!=level){
      vector<string> nowfile;
      utils::scanDir(dirPrefix+"/level-"+std::to_string(nowlevel),nowfile);
      vector<SSTable> nowmergefile;
      int nowsize=nowfile.size();
      sort(nowfile.begin(),nowfile.end(),comp);
      //当超出当前层应有大小时
      if(nowsize>pow(2,(nowlevel+1))){
          //获取所有应该归并内容
          for(int i=0;i<nowsize-pow(2,(nowlevel+1));i++){
              string fileName=dirPrefix+"/level-"+std::to_string(nowlevel)+"/"+nowfile[i];
              SSTable tmp=getSSTable(fileName);
              storeData.erase(fileName);
              if(utils::rmfile(fileName.c_str())){
                  cout<<"failed to delete "<<fileName<<endl;
                }
              nowmergefile.push_back(tmp);
            }
          //非最底层时
          if(nowlevel!=level-1){
              uint64_t levelrange=nowmergefile.size();
              uint64_t min0=nowmergefile[0].head.min;
              uint64_t max0=nowmergefile[0].head.max;
              for(uint64_t i=1;i<levelrange;i++){
                  if(nowmergefile[i].head.min<min0)
                    min0=nowmergefile[i].head.min;
                  if(nowmergefile[i].head.max>max0)
                    max0=nowmergefile[i].head.max;
                }
              std::vector<string> nextlevel;
              utils::scanDir(dirPrefix+"/level-"+std::to_string(nowlevel+1),nextlevel);
              uint64_t nextlevelsize=nextlevel.size();

              //找到所有应该归并的内容
              for(uint64_t i=0;i<nextlevelsize;i++){
                  nextlevel[i]=dirPrefix+"/level-"+std::to_string(nowlevel+1)+"/"+nextlevel[i];
                  if((((storeData[nextlevel[i]].head.min)<=max0)&&((storeData[nextlevel[i]].head.min)>=min0))
                     ||(((storeData[nextlevel[i]].head.max)<=max0)&&((storeData[nextlevel[i]].head.max)>=min0))){
                      SSTable tmp=getSSTable(nextlevel[i]);
                      storeData.erase(nextlevel[i]);
                      if(utils::rmfile(nextlevel[i].c_str())){
                          cout<<"failed to delete "<<nextlevel[i]<<endl;
                        }
                      nowmergefile.push_back(tmp);
                    }
                }
              //开始归并
              vector<SSTable> result;

              uint64_t resulttime=nowmergefile[0].head.time;
              //生成这次所有生成的header里面的time
              for(uint64_t i=1;i<levelrange;i++){
                  if(nowmergefile[i].head.time>resulttime)
                    resulttime=nowmergefile[i].head.time;
                }
              if(!utils::dirExists(dirPrefix+"/level-"+std::to_string(nowlevel+2)))
                result=merge(nowmergefile,true);
              else
                result=merge(nowmergefile,false);
              int resultsize=result.size();
              string path=dirPrefix+"/level-"+std::to_string(nowlevel+1);
              std::vector<string> nextfile;
              utils::scanDir(path,nextfile);

              int j=0;
              for(int i=0;i<resultsize;i++){
                  result[i].head.time=resulttime;
                  while(find(nextfile.begin(),nextfile.end(),std::to_string(resulttime)+"-"+std::to_string(j)+".sst")!=nextfile.end()){
                      j++;
                    }
                  path=dirPrefix+"/level-"+std::to_string(nowlevel+1)+"/"+std::to_string(resulttime)+"-"+std::to_string(j)+".sst";
                  result[i].writeDisk(path);
                  result[i].value="";
                  storeData.insert(pair<string,SSTable>(path,result[i]));
                  j++;
                }
            }

          else if(nowlevel==level-1){
              if(!utils::dirExists(dirPrefix+"/level-"+std::to_string(level))){
                  utils::mkdir((dirPrefix+"/level-"+std::to_string(level)).c_str());
                  uint64_t resulttime=nowmergefile[0].head.time;
                  int timesize=nowmergefile.size();
                  //生成这次所有生成的header里面的time
                  for(int i=1;i<timesize;i++){
                      if(nowmergefile[i].head.time>resulttime)
                        resulttime=nowmergefile[i].head.time;
                    }
                  //开始归并
                  vector<SSTable>result=merge(nowmergefile,true);
                  int resultsize=result.size();

                  for(int i=0;i<resultsize;i++){
                      result[i].head.time=resulttime;
                      string path=dirPrefix+"/level-"+std::to_string(level)+"/"+std::to_string(resulttime)+"-"+std::to_string(i)+".sst";
                      result[i].writeDisk(path);
                      result[i].value="";
                      storeData.insert(pair<string,SSTable>(path,result[i]));
                    }
                  level++;
                  break;
                }
            }

        }
      nowlevel++;
    }
}

//把文件中的SSTable读到内存里
SSTable KVStore::getSSTable(string fileName){
  SSTable tmp;
  std::ifstream istrm(fileName, std::ios::binary);
  if (!istrm.is_open()) {
      std::cout << "failed to open "<<fileName<< '\n';
    } else {
      istrm.read(reinterpret_cast<char*>(&(tmp.head)), 32);
      istrm.read(reinterpret_cast<char*>(&(tmp.bf)), 10240);
      uint64_t si2=tmp.head.num;
      for(uint64_t i=0;i<si2;i++){
          Index tmpindex;
          istrm.read(reinterpret_cast<char*>(&(tmpindex.key)), 8);
          istrm.read(reinterpret_cast<char*>(&(tmpindex.offset)), 4);
          tmp.index.push_back(tmpindex);
        }
      istrm>>tmp.value;
    }
  return tmp;
}

//对文件按时间戳进行排序
bool filecomp(SSTable & a, SSTable & b) {
        return a.head.time>b.head.time;
}

//合并同层文件
SSTable KVStore::mergelevel(vector<SSTable> &mergefile,bool isBottom){
  int fileSize=mergefile.size()-1;
  sort(mergefile.begin(),mergefile.end(),filecomp);
  //开始归并
  while(fileSize>0){
      int count=0;
      int i=0;
      while(i<fileSize-1){
          mergefile[count]=mergeTwo(mergefile[i],mergefile[i+1],isBottom);
          i +=2;
          count++;
        }
      if(fileSize%2==0)
        mergefile[count]=mergefile[fileSize];
      else
        mergefile[count]=mergeTwo(mergefile[fileSize-1],mergefile[fileSize],isBottom);
      fileSize=count;
    }
  //生成的大SSTable
  SSTable largeSS=mergefile[0];

//  //把大SSTable切分
//  vector<SSTable> result=divide(largeSS);
  return largeSS;
}

//合并所有文件
vector<SSTable> KVStore::merge(vector<SSTable> &mergefile,bool isBottom){
  sort(mergefile.begin(),mergefile.end(),filecomp);
  vector<vector<SSTable>> tmpmergefile;
  int fsize=mergefile.size();
  vector<SSTable> tmp;
  uint32_t tmptime=mergefile[0].head.time;
  //同层文件合并
  for(int i=0;i<fsize;i++){
      if(mergefile[i].head.time==tmptime){
          tmp.push_back(mergefile[i]);
        }
      else{
          tmpmergefile.push_back(tmp);
          tmptime=mergefile[i].head.time;
          tmp.clear();
          tmp.push_back(mergefile[i]);
        }
    }
   tmpmergefile.push_back(tmp);
  mergefile.clear();
  SSTable t;
  int ssize=tmpmergefile.size();
  for(int i=0;i<ssize;i++){
      t=mergelevel(tmpmergefile[i],isBottom);
      mergefile.push_back(t);
    }

  int fileSize=mergefile.size()-1;
  sort(mergefile.begin(),mergefile.end(),filecomp);

  //开始归并不同层文件形成的大SSTable
  while(fileSize>0){
      int count=0;
      int i=0;
      while(i<fileSize-1){
          mergefile[count]=mergeTwo(mergefile[i],mergefile[i+1],isBottom);
          i +=2;
          count++;
        }
      if(fileSize%2==0)
        mergefile[count]=mergefile[fileSize];
      else
        mergefile[count]=mergeTwo(mergefile[fileSize-1],mergefile[fileSize],isBottom);
      fileSize=count;
    }
  //生成的大SSTable
  SSTable largeSS=mergefile[0];

  //把大SSTable切分
  vector<SSTable> result=divide(largeSS);
  return result;
}

//二路归并两个文件
//如果当前是最底层 需要删除所有~DELETED~内容
SSTable KVStore::mergeTwo(SSTable table1,SSTable table2,bool isBottom){
  vector<SSTable> mergefile;
  mergefile.push_back(table1);
  mergefile.push_back(table2);
  //保存文件的时间戳信息
  uint64_t time1=table1.head.time;
  uint64_t time2=table2.head.time;
  uint64_t time=0;
  if(time1>time2)
    time=time1;
  else
    time=time2;

  SSTable result;
  string tmpvalue;
  uint64_t tmpoffset=0;
  uint64_t ttmp1=0;
  uint64_t ttmp0=0;
  Index tmpindex;
  string valueforresult="";
  vector<Index> indexforresult;

  vector<Index> in0=mergefile[0].index;
  vector<Index> in1=mergefile[1].index;
  auto k0=in0.begin();
  auto k1=in1.begin();
  uint64_t tmpoff=0;
  while(!in0.empty()&&!in1.empty()){
      k0=in0.begin();
      k1=in1.begin();
      tmpvalue="";
      if((k0->key<k1->key)||((k0->key==k1->key)&&mergefile[0].head.time>mergefile[1].head.time)){
          if(!(k0->key<k1->key))
            in1.erase(in1.begin());
          //读内容
          k0=in0.begin();
          auto tmp=k0;
          uint64_t pos=k0->offset;

          tmpindex.key=k0->key;
          k0++;

          if(k0==(in0.end())){
              tmpoff=valueforresult.size();
              tmpindex.offset=tmpoff;
              tmpvalue += mergefile[0].value.substr(pos);
              tmpindex.offset=pos;
            }
          else{
              uint64_t len=k0->offset-tmp->offset;
              tmpoff=valueforresult.size();
              tmpindex.offset=tmpoff;
              tmpvalue += mergefile[0].value.substr(pos,len);
            }
          in0.erase(tmp);
          if(!isBottom){
              tmpoffset +=tmpvalue.size();
              ttmp0 +=tmpvalue.size();
              tmpoff=valueforresult.size();
              tmpindex.offset=tmpoff;
              valueforresult += tmpvalue;
              indexforresult.push_back(tmpindex);
            }
          else if(tmpvalue!="~DELETED~"){
              tmpoffset +=tmpvalue.size();
              ttmp0 +=tmpvalue.size();
              tmpoff=valueforresult.size();
              tmpindex.offset=tmpoff;
              valueforresult += tmpvalue;
              indexforresult.push_back(tmpindex);
            }
        }
      else if((k0->key>k1->key)||((k0->key==k1->key)&&mergefile[0].head.time<mergefile[1].head.time)){
          if(!(k0->key>k1->key))
            in0.erase(in0.begin());
          //读内容
          k1=in1.begin();
          auto tmp=k1;
          uint64_t pos=k1->offset;

          tmpindex.key=k1->key;

          k1++;
          if(k1==in1.end()){
              tmpvalue += mergefile[1].value.substr(pos);
              tmpindex.offset=pos;
            }
          else{
              uint64_t len=k1->offset-tmp->offset;
              tmpvalue += mergefile[1].value.substr(pos,len);
            }
          in1.erase(tmp);
          if(!isBottom){
              tmpoffset +=tmpvalue.size();
              ttmp1 +=tmpvalue.size();
              tmpoff=valueforresult.size();
              tmpindex.offset=tmpoff;
              valueforresult += tmpvalue;
              indexforresult.push_back(tmpindex);
            }
          else if(tmpvalue!="~DELETED~"){
              tmpoffset +=tmpvalue.size();
              ttmp1 +=tmpvalue.size();
              tmpoff=valueforresult.size();
              tmpindex.offset=tmpoff;
              valueforresult += tmpvalue;
              indexforresult.push_back(tmpindex);
            }
        }
      else if((k0->key==k1->key)&&mergefile[0].head.time==mergefile[1].head.time){
          in1.erase(in1.begin());
          continue;
        }
    }
  if(in0.empty()){
      if(!isBottom){
          while(!in1.empty()){
              tmpvalue="";
              auto t=in1.begin();
              tmpindex.key=t->key;
              auto tnext=t+1;
              if(tnext==in1.end()){
                  tmpvalue += mergefile[1].value.substr(t->offset);
                }
              else{
                  uint64_t len=tnext->offset-t->offset;
                  tmpvalue += mergefile[1].value.substr(t->offset,len);
                }
              tmpoffset +=tmpvalue.size();
              ttmp1 +=tmpvalue.size();
              tmpoff=valueforresult.size();
              tmpindex.offset=tmpoff;
              valueforresult += tmpvalue;
              indexforresult.push_back(tmpindex);
              in1.erase(t);
            }
        }
      else{
          while(!in1.empty()){
              tmpvalue="";
              auto t=in1.begin();
              tmpindex.key=t->key;
              auto tnext=t+1;
              ttmp1=t->offset;
              if(tnext==in1.end()){
                  tmpvalue += mergefile[1].value.substr(t->offset);
                }
              else{
                  uint64_t len=tnext->offset-t->offset;
                  tmpvalue += mergefile[1].value.substr(t->offset,len);
                  ttmp1 +=tmpvalue.size();
                }
              if(tmpvalue!="~DELETED~"){
                  tmpoffset +=tmpvalue.size();
                  tmpoff=valueforresult.size();
                  tmpindex.offset=tmpoff;
                  valueforresult += tmpvalue;
                  indexforresult.push_back(tmpindex);
                }
              in1.erase(t);
            }
        }
    }

  else if(in1.empty()){
      if(!isBottom){
          while(!in0.empty()){
              tmpvalue="";
              auto t=in0.begin();
              tmpindex.key=t->key;
              auto tnext=t+1;
              if(tnext==in0.end()){
                  tmpvalue += mergefile[0].value.substr(t->offset);
                }
              else{
                  uint64_t len=tnext->offset-t->offset;
                  tmpvalue += mergefile[0].value.substr(t->offset,len);
                }
              tmpoffset +=tmpvalue.size();
              ttmp0 +=tmpvalue.size();
              tmpoff=valueforresult.size();
              tmpindex.offset=tmpoff;
              valueforresult += tmpvalue;
              indexforresult.push_back(tmpindex);
              in0.erase(t);
            }
        }
      else{
          while(!in0.empty()){
              tmpvalue="";
              auto t=in0.begin();
              tmpindex.key=t->key;
              auto tnext=t+1;
              ttmp1=t->offset;
              if(tnext==in0.end()){
                  tmpvalue += mergefile[0].value.substr(t->offset);
                }
              else{
                  uint64_t len=tnext->offset-t->offset;
                  tmpvalue += mergefile[0].value.substr(t->offset,len);
                  ttmp0 +=tmpvalue.size();
                }
              if(tmpvalue!="~DELETED~"){
                  tmpoffset +=tmpvalue.size();
                  ttmp0 +=tmpvalue.size();
                  tmpoff=valueforresult.size();
                  tmpindex.offset=tmpoff;
                  valueforresult += tmpvalue;
                  indexforresult.push_back(tmpindex);
                }
              in0.erase(t);
            }
        }
    }
  result.head.time=time;
  result.index=indexforresult;
  result.value=valueforresult;
  valueforresult="";
  indexforresult.clear();
  return result;
}

//将大的SSTable拆分成2MB的SSTable
vector<SSTable> KVStore::divide(SSTable source){
  uint64_t bytes=10272;
  uint64_t start=0;
  vector<Index> sourceindex=source.index;
  uint64_t indexsize=sourceindex.size();
  SSTable rtmp;
  vector<Index> rindex;
  BloomFilter rbf;
  vector<SSTable> result;
  uint64_t i=0;
  for(;i<indexsize-1;i++){
      bytes += 12+sourceindex[i+1].offset-sourceindex[i].offset;
      //如果到达一个SSTable上限
      if(bytes>2097152){
          //      if(bytes>(2097152/200)){
          rtmp.head.min=sourceindex[start].key;
          rtmp.head.max=sourceindex[i-1].key;
          rtmp.head.num=i-start;
          rindex.assign(sourceindex.begin()+start,sourceindex.begin()+i);
          uint64_t len=source.index[i].offset-source.index[start].offset;
          rtmp.value= source.value.substr(source.index[start].offset,len);
          uint64_t offsetgap=0;
          if(rindex[0].offset>(32+10240+12*rtmp.head.num))
            offsetgap=rindex[0].offset;
          uint64_t asize=rindex.size();
          for(uint64_t k=0;k<asize;k++){
              rindex[k].offset -= offsetgap;
              rbf.add(rindex[k].key);
            }
          rtmp.index=rindex;
          rindex.clear();
          rtmp.bf=rbf;
          start=i;
          if(i>1)
            i=i-1;
          bytes=10272;
          result.push_back(rtmp);
        }
    }

  bytes += 12+source.value.size()-sourceindex[i].offset;
  if(bytes>(2097152)){
      rtmp.head.min=sourceindex[start].key;
      rtmp.head.max=sourceindex[i-1].key;
      rtmp.head.num=i-start;
      rindex.assign(sourceindex.begin()+start,sourceindex.begin()+i);

      uint64_t len=source.index[i].offset-source.index[start].offset;
      rtmp.value= source.value.substr(source.index[start].offset,len);
      uint64_t offsetgap=rindex[0].offset;
      uint64_t asize=rindex.size();
      for(uint64_t k=0;k<asize;k++){
          rindex[k].offset -= offsetgap;
          rbf.add(rindex[k].key);
        }
      rtmp.index=rindex;
      rindex.clear();
      start=i;
      rtmp.bf=rbf;
      bytes=10272;
      result.push_back(rtmp);

      //最后一个自成一个SSTable
      rtmp.head.min=sourceindex[i].key;
      rtmp.head.max=sourceindex[i].key;
      rtmp.head.num=1;
      rindex.assign(sourceindex.begin()+i,sourceindex.begin()+i+1);

      rtmp.value= source.value.substr(source.index[i].offset);
      asize=rindex.size();
      for(uint64_t k=0;k<asize;k++){
          rindex[k].offset=0;
          rbf.add(rindex[k].key);
        }
      rtmp.index=rindex;
      rindex.clear();
      rtmp.bf=rbf;
      bytes=10272;
      result.push_back(rtmp);
    }
  else{
      //最后一个和之前剩的一起合成一个SSTable
      rtmp.head.min=sourceindex[start].key;
      rtmp.head.max=sourceindex[i].key;
      rtmp.head.num=i-start+1;
      rindex.assign(sourceindex.begin()+start,sourceindex.end());

      rtmp.value= source.value.substr(source.index[start].offset);
      uint64_t offsetgap=rindex[0].offset;
      uint64_t asize=rindex.size();
      for(uint64_t k=0;k<asize;k++){
          rindex[k].offset -= offsetgap;
          rbf.add(rindex[k].key);
        }
      rtmp.index=rindex;
      rindex.clear();
      rtmp.bf=rbf;
      bytes=10272;
      result.push_back(rtmp);
    }
  return result;
}
