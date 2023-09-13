# 6.5840
## 文件结构/file structure
### lab1：MapReduce实现单词计数
核心代码处于`/src/mr`下，`coordinator.go`中，编写master逻辑。`rpc.go`中，编写rpc通信的内容结构，`worker.go`中编写worker的逻辑。

### lab2：Raft共识算法实现
核心代码处于`/src/raft`下的`raft.go`中。

### lab3：使用raft实现简单的kv
核心代码处于`/src/kvraft`下。其中`client.go`是kv客户端，在应用程序中使用。`server,go`是服务端，进行存储，共识复制等实现。

### lab4：实现分片且可配置的kv-raft

基于Raft的分片配置相关服务在`/src/shardctrler`下，同样存在client与server。client中，实现了配置更改，配置查询等接口的提供。server中进行实际数据的存储及更改逻辑实现。

基于Raft的分片kv在`/src/shardkv`下，同样有client以及server。

## 博客/blogs
lab1 通过10次测试

lab2 通过10000次测试

lab3 通过10000次测试

lab4包括challenge 通过10000次测试

博客地址

lab4: https://www.cnblogs.com/alyjay/p/17555050.html

lab3: https://www.cnblogs.com/alyjay/p/17516454.html

lab2: https://www.cnblogs.com/alyjay/p/17451827.html


此仓库将在2023.9.20关闭
