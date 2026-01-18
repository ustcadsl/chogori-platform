## 分离式内存图数据库运行说明

### 源代码

https://github.com/LiTwotwo/Grasper.git

### 数据集示例

我们在文件夹/data中提供了一个示例数据集，包含四个子文件夹：`/vertices`、`/vtx_property`、`/edge_property `和`/index`，分别存储顶点的数据（图拓扑）、顶点属性、边属性和索引信息。

#### 数据格式

* `/vertices`：每个文件包含多行，其中每行表示一个顶点及其邻居列表。

  ```
  {vid} \tab {num_in_neighbors} \tab {neighbor1} \s {neighbor2} \s ... {neighborN} \s {num_out_neighbors} \tab {neighbor1} \s {neighbor2} \s ... {neighborN} \s
  ```

* `/vtx_property`：每个文件包含多行，其中每行表示一个顶点及label和属性信息（键值对列表）。

  ```
  {vid} \tab {label} \tab [vp_key1:val,vp_key2:val...]
  ```

* `/edge_property`：每个文件包含多行，其中每行表示一条边及其label和属性信息（键值对列表）。

  ```
  {in_vid} \tab {out_vid} \tab {label} \tab [ep_key1:val,ep_key2:val...]
  ```

* 子文件夹`/index`，包含四个文件 `vtx_label`, `edge_label`, `vtx_property_index`, `edge_property_index`，用于记录string id映射，该映射将label从string转换为int以进行数据压缩。

   `vtx_label`, `edge_label`的每一行格式如下：

  ```bash
  #string \tab int
  #for example:
  person	1
  software	2
  ```

  `vtx_property_index` 和 `edge_property_index` 的每一行格式如下：

  ```bash
  #string \tab int
  #for example:
  name	1
  age 	2
  lang	3
  ```

### 数据集上传至HDFS

继承Grasper的数据输入部分，从HDFS读取数据，运行`dataload.sh`完成数据上传至HDFS，将目标文件夹修改为指定数据集。 

### 运行

1. 修改 `grasper-conf.ini`, `workers.cfg` 和 `ib.cfg`

   **grasper-conf.ini**： 包含图数据库配置参数

   **workers.cfg**：存储节点需要的通信配置，以行为单位，格式为`hostname:ibname:tcp_port:ib_port`

   **ib.cfg**：计算节点需要的通信配置，以行为单位，格式为`hostname:ibname:tcp_port:ib_port`

2. 为了完成客户端查询，需要启动存储节点和计算节点

   启动remote 完成数据载入：

   ```sh
   $ sh $GRASPER_HOME/start-remote.sh workers.cfg
   ```

   启动server 完成初始化，并于存储节点进行元数据同步（计算节点使用mpi并行框架，需要指定参数`$NUM_of_Server`），等待客户端连接：

   ```sh
   $ sh $GRASPER_HOME/start-server.sh {$NUM_of_Server+1} ib.cfg
   
   # sample initialization log
   Node: { world_rank = 0 world_size = 2 local_rank = 0 local_size = 1 color = 0 hostname = localhost ibname = 10.0.0.64}
   Node: { world_rank = 1 world_size = 2 local_rank = 0 local_size = 1 color = 1 hostname = localhost ibname = 10.0.0.64}
   given SNAPSHOT_PATH = /tmp/sf0.1/snapshpt, processed = /tmp/sf0.1/snapshpt
   DONE -> Local Config->Init()
   DONE -> Local Config->Init()
   Master bind addr:tcp://*:8011
   Receiver bind addr:tcp://*:9091
   Remote listener connect addr:tcp://localhost:10011
   local_rank_ == 0  node 0: cores: [ 0 2 4 6 8 10 12 14 16 18], [ 20 22 24 26 28 30], [ 32 34 36 38], [ 1 3 5 7 9 11], [ 13 15 17 19 21 23 25], [ 27 29 31 33 35 37 39],  threads: [ 0 8 2 16 14 20 28 22 36 34 40 48 42 56 54 60 68 62 76 74], [ 1 9 3 17 15 21 41 49 43 57 55 61], [ 29 23 37 35 69 63 77 75], [ 4 6 12 18 10 24 44 46 52 58 50 64], [ 26 32 38 30 5 7 13 66 72 78 70 45 47 53], [ 19 11 25 27 33 39 31 59 51 65 67 73 79 71], 
   Worker0: DONE -> Init Core Affinity
   Worker0: DONE -> Register LOCAL MEM, SIZE = 41943040
   RDMA Ctrl create success!
   Success on binding: 9090
   RDMA open device!
   remote mr key = 266240 remote mr buf ptr = 140379543048208
   Get remote data!
   Alloc memory region!
   INFO: initializing RDMA done (16 ms)
   Worker0: DONE -> Mailbox->Init()
   Worker0: DONE -> Parser_->LoadMapping()
   Worker0: DONE -> Get String index()
   Worker0: DONE -> expert_adapter->Start()
   Grasper Servers Are All Ready ...
   ```

3. 启动client，使用Gremlin查询语句查询

   ```sh
   $ sh $GRASPER_HOME/start-client.sh ib.cfg
   
   #sample log
   Grasper> Grasper -q g.V().has("id", 326062).union(properties("firstName", "lastName", "birthday", "locationIP", "browserUsed"), out("isLocatedIn").properties("name"))
   [Client] Processing query : g.V().has("id", 326062).union(properties("firstName", "lastName", "birthday", "locationIP", "browserUsed"), out("isLocatedIn").properties("name"))
   [Client] Client just posted a REQ
   [Client] Client 1 recvs a REP: get available worker_node0
   
   [Client] Client posts the query to worker_node0
   
   [Client] result: Query 'g.V().has("id", 326062).union(properties("firstName", "lastName", "birthday", "locationIP", "browserUsed"), out("isLocatedIn").properties("name"))' result: 
   =>{firstName:Carmen}
   =>{lastName:Lepland}
   =>{birthday:1984-02-18}
   =>{locationIP:195.20.151.175}
   =>{browserUsed:Internet Explorer}
   =>{name:Tallinn}
   [Timer] 1466.62 ms for ProcessQuery
   
   [Timer] 1468 ms for whole process.
   ```

