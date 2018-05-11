# Name_Dis_Sys
## 预处理

preprocess.batch_preprocess(author_names,directory)

输入是要处理的author_names的list，directory是预处理文件输出的文件夹

第一步：将database中的整个表下载到/tmp/目录下

```python
db = DatabaseClient()
db.get_aname2aid_from_db()
db.get_aid2pid_affid_from_db()
db.get_pid2ptitle_cid_jid_year()
```
第二步：从下载好的表的文件做成大字典，大字典内容是需要批量处理的author_names

```python
name2aid,aid2name = get_name2aid_from_file(aims = author_names) 
# name2aid：name 到author_id字典，  aid2name: author_id到name的字典
aid2pid_affid,pid2aid = get_aid2pid_affid_from_file(aims = aid2name)
# aid2pid_affid： author id到 affiliatioin id，paper id， pid2aid： paper id 到author id
pid2ptitle_cid_jid_year = get_pid2ptitle_cid_jid_year_from_file(aims = pid2aid)
#paper id 到title 会议 期刊 发表年 
```






第三步：对于author_names中的单个author_name，从大字典中抽出这个人名需要的内容做成字典和list

```python
paper_affiliations,authors_dic,title_venue_year_dic = batch_dic_extract(author_name,name2aid,aid2name,aid2pid_affid,pid2aid,pid2ptitle_cid_jid_year)
#paper_affiliations,authors_dic,title_venue_year_dic 是单个人需要的dict数据
#之后要加aim paper只需要 对这三个字典添加就可以了
#paper_affiliations 是一个list，每个元素里是一个三元组PaperID,AffiliationID,AuthorID
#authors_dic 是paper_id->authors name
#title_venue_year_dic 是paper_id ->title_venue_year
```

batch_dic_extract 函数的输入是第二步做好的大字典，以及author_name，输出是这个人名做预处理需要的dict数据。

第四步：将单个人名的dict数据输入到batch_analyze_papers_and_init_clusters，返回预处理文件，然后把这些文件dump到本地就可以扔进算法里了。

```python
papers, clusters, paper_idx_2_cluster_id, inverted_indices, author_id_set = batch_analyze_papers_and_init_clusters(author_name,paper_affiliations,authors_dic,title_venue_year_dic)

```







## 系统流程

1.  数据预处理（preprocess)：
  生成类(lib)，然后导入数据库
  类的定义：paper, cluster肯定存，异构网络是否存下来？
2.  读取数据库到本地(data文件夹，databaseclient)，算法读本地比较快
3.  算法部分（algorithm)
4.  导入数据库部分(databaseclient)

## 系统组成

* lib:

  DisData: 定义数据类型 Paper Cluster （周选益）

  DatabaseClient: 定义数据库交互工具  （陆逸凡）

  util: 放一些常常用的小工具，暂时空着 想起来放进去

* algorithm:

  算法放这

* test:

  算法的测试部分

* encode(是否放入lib？）:

  编码不重复即可

* preprocess:

  预处理工具，将一些数据结构提前导入到数据库 （周选益）

* data：

  数据从数据库读取放到这里

##5月1日更新

现在预处理流程为：

* 用DatabaseClient.get_aname2aid_from_db 将整个大表全部导入到文件file.
* 用preprocess.get_name2aid_from_file读取本地文件中的大表，做成字典。

目前整个预处理流程只需要几小时

