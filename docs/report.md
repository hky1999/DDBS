# DDBS

田凯夫 2023310805

## 构建和运行项目

hadoop 搭建参考仓库 https://github.com/big-data-europe/docker-hadoop 。

用 docker-compose 配置 hadoop ，读写文件需要暴露 datanode 的端口，需要修改 /etc/hosts 并将其映射至 127.0.0.1 。

在项目目录下执行 `./prepare.sh` 安装依赖工具。

执行 `./run_docker.sh` 运行 docker 容器（端口默认给定，可能出现冲突）。

执行 `python -m server.server --init` 将预先生成好的数据分块并导入到 DBMS 和 HDFS 中。

执行 `python -s tests/test_server.py` 运行测试。

## 项目架构

![](overview.svg)

- Server 对数据进行划分和布局，接收用户请求并制定查询方案
- Hadoop FS 可以对文章内容等大文件进行保存，减少本地的存储负担
- MongoDB 是 NoSQL 数据库，使用文档类型保存数据，具备良好的扩展性和丰富的查询方法（MQL）
- Redis 内存数据库用于对 MongoDB 中的数据进行缓存，加快查询的响应，减少对存储设备频繁的访问

## 功能简介

- 查询、插入、更新用户数据
- 查询某用户阅读历史
- 查询某文章详细内容，包括文本、图片和视频
- 获取某天、某周、某月内最受欢迎的文章信息
- 可实时监控数据库的状态

## 实现细节



## 分工

田凯夫：系统搭建、前端测试

