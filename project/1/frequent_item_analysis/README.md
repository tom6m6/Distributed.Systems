# Frequent item analysis

## 难易程度： **中

## 待完成

- 请在DSPPCode.mapreduce.frequent_item_analysis.impl中创建FrequentItemAnalysisMapperImpl，继承FrequentItemAnalysisMapper，实现抽象方法
- 请在DSPPCode.mapreduce.frequent_item_analysis.impl中创建FrequentItemAnalysisRunnerImpl，继承FrequentItemAnalysisRunner，实现抽象方法
- 请在DSPPCode.mapreduce.frequent_item_analysis.impl中创建FrequentItemAnalysisReducerImpl，继承FrequentItemAnalysisReducer，实现抽象方法
- 请在DSPPCode.mapreduce.frequent_item_analysis.SortHelperImpl，SortHelper，实现抽象方法

## 问题描述

频繁项挖掘是一种数据挖掘技术，旨在找出数据集中经常一起出现的项集合。这些项集合被称为频繁项集，可以用于发现数据中的模式、关联和规律。


在频繁项挖掘中，有两个概念分别是阶，支持度。


- 支持度：指在给定的数据集中，包含某个特定项集的事务数量与总事务数量的比例。计算支持度的公式如下：
$$\text{Support}(A) = \frac{\text{Number of transactions containing }A}{\text{Total number of transactions}}$$
其中$A$是一个频繁项集，$\text{Number of transactions containing }A$是包含项集$A$的事务数量，$\text{Total number of transactions}$是数据库中的总事务数量。


- 阶：指的是项集的规模，即项集包含的项的数量。例如，一阶频繁项集是指包含单个项的集合，二阶频繁项集是指包含两个项的集合，依此类推。$n$阶频繁项集就是指那些包含$n$个项的频繁项集。



例如，在数据库中，我们有如下几行交易数据：

```
crackers,bread,banana 
crackers,coke,butter,coffee 
crackers,bread 
crackers,bread 
crackers,bread,coffee 
butter,coke 
butter,coke,bread,crackers 
```

同时设频繁项的支持度$\text{Support = 0.5}$则我们可以挖掘到下述频繁项集：
- 一阶频繁项集：{crackers}, {bread}
- 二阶频繁项集：{crackers,bread}
- 三阶频繁项集：无

现给予一个交易记录文件，请使用MapReduce挖掘其$n$阶频繁项

输入格式：

输入一共一个文件Transactions，每一行记录一次交易中所购买的商品，不同商品使用英文逗号","隔开。


Runner的run方法输入String数组，长度为5，分别是输入路径，输出路径，阶数，支持度，行数


```
crackers,bread,banana 
crackers,coke,butter,coffee 
crackers,bread 
crackers,bread 
crackers,bread,coffee 
butter,coke 
butter,coke,bread,crackers 
```
  


输出格式：输出满足支持度$n$阶频繁项集合，其中频繁项内部使用字典序升序排序，不同商品使用英文逗号","隔开。一行输出一个频繁项。在$n = 2,\text{support}=0.5$情况下输出为
 
```
bread,crackers
```
