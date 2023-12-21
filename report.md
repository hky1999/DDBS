# DDBS

## LOG

hadoop 搭建参考仓库 https://github.com/big-data-europe/docker-hadoop 。

用 docker-compose 配置 hadoop ，读写文件需要暴露 datanode 的端口，需要修改 /etc/hosts 并将其映射至 127.0.0.1 。
