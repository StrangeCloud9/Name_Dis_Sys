# Name_Dis_Sys
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

