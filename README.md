# FlinkArgus

## 目录

[TOC]

## 简介

​	基于Flink的数据同步工具，具有插件化、扩展性等特点，针对数据同步，抽象Reader、Channel、Writer三组件。基于FlinkInputFormatSource、MapFunction、FlinkOutputFormatSink进行扩展。



## 快速开始

- clone本项目

  ```shell
  git clone git@github.com:WeiWan5675/Flink_Argus.git
  ```

  

- 编译

  ```shell
  mvn clean install -DSpikTest
  ```
  
  
  
- 解压缩

  在Arugs_Start工程下,编译出FlinkArgus-${version}.tar.gz,解压得到FlinkArgus-${version}

- 启动

  ```shell
  $FLINK_ARUGS/bin/FlinkArgus.sh -aconf "argus-default.yaml"
  ```

  - 支持的参数

  | 参数名称    | 参数简写    | 必要  | 解释                                  |
  | ----------- | ----------- | ----- | ------------------------------------- |
  | cmdMode     | -cmd        | FALSE | 默认FALSE，打开命令模式               |
  | mode        | -mode       | TRUE  | 工作模式                              |
  | flinkConf   | -fconf      | FALSE |                                       |
  | hadoopConf  | -hconf      | FALSE |                                       |
  | argusConf   | -aconf      | TRUE  | 需要指定任务配置文件【CMD模式不需要】 |
  | yarnQueue   | -yq\|-queue | FALSE | YARN任务队列                          |
  | parallelism | -p          | FALSE | 任务并行度                            |



## 工程结构

```
.FlinkArgus-1.0.0
├── bin
│   └── FlinkArgus.sh
├── conf
│   └── argus-default.yaml
├── lib
├── plugins
│   ├── channel
│   │   └── common_channel-1.0.0.jar
│   ├── reader
│   │   └── mysql_reader-1.0.0.jar
│   └── writer
│       └── hive_writer-1.0.0.jar
└── README.md
```



## 支持列表

- **Reader**

  | 插件名称    | 文档 | 备注          |
  | ----------- | ---- | ------------- |
  | MysqlReader |      | Mysql读取插件 |
  |             |      |               |
  |             |      |               |

  

- **Writer**

  | 插件名称   | 文档 | 备注       |
  | ---------- | ---- | ---------- |
  | HiveWriter |      | Hive写插件 |
  |            |      |            |
  |            |      |            |

  

- **Channel**

  | 插件名称      | 文档 | 备注                   |
  | ------------- | ---- | ---------------------- |
  | CommonChannel |      | 空实现，默认不处理数据 |
  |               |      |                        |
  |               |      |                        |

  



## 其它

​	本项目起源于Flinkx，扩展了Channel组件，以及CMD模式  并针对个人使用过程中一些情况做特殊处理。