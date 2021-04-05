
# Pythonic Hadoop
本项目使用Python模拟了Hadoop运行环境，采用与Hadoop相似的API，可以使用Python完成MapReduce任务（job）。

## 背景
### MapReduce是什么
1. MapReduce是一种用于处理大数据的设计思想。主要用于搜索领域，解决海量数据的计算问题。
2. MR有两个阶段组成：Map和Reduce，用户只需实现map()和reduce()两个函数，即可实现分布式计算。
3. MapReduce的中心思想就是**分而治之**。Map负责“分”，把复杂的任务分解为若干个“简单的任务”来处理；Reduce负责“结果汇总”。

### MapReduce过程

![alt mapreduce](/resource/mapreduce.jpg)
1. 第一步对输入的数据进行切片，每个切片分配一个Map任务，Map对其中的数据进行计算，对每个数据用键值对（key-value）的形式记录，然后输出到环形缓冲区（图中sort的位置）。
2. 在Shuffle/Sort阶段对第一步输出的数据进行排序与归并，这一阶段的数据以中间结果的形式保存在临时文件中。
3. 框架把中间结果传到Reduce中来，合并中间结果，最终输出。

### Hadoop是什么

[Apache Hadoop](https://hadoop.apache.org/)是一个实现MapReduce算法的分布式数据处理应用程序的框架。使用Hadoop构建的应用程序都分布在集群计算机商业大型数据集上运行。

## 介绍

由于Hadoop是Java语言编写的，对于没有Java背景的同学来说理解较为困难，因此本项目提供了一个Hadoop API的模拟环境，在不需要额外安装Java和Hadoop的情况下学习Map和Reduce的使用。

### 对比Hadoop

为了实现简单，我们的Hadoop环境Apache Hadoop相比由诸多的简化。

1. 我们的环境不提供分布式计算能力，因此Map和Reduce阶段都是单线程执行的。
2. Reduce的结果只输出到一个文件，而不是图中可能由多个结果文件。不过我们的系统可以接收多个文件作为输入。


## 快速开始

1. 克隆仓库到本地

```
git clone https://github.com/DoubleVII/python_hadoop
```
2. `demo_wordcount.py`提供了一个简单的WordCount示例，运行demo
```
python demo_wordcount.py
```
3. 输出结果为`True`表示运行成功，程序的处理结果可以在`/demo_data/test_output.txt`中查看。

## 文档
[说明文档](/doc/README.md)介绍了基本的搭建MapReduce任务的方法以及各种扩展功能。

## 作业
吓唬你的，作业在教学立法上，也没这么复杂。
