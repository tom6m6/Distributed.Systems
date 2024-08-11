# Common PageRank

## 难易程度:  *** 难

## 待完成
- 请在DSPPCode.mapreduce.common_pagerank.impl中创建PageRankJoinMapperImpl，继承PageRankJoinMapper，实现抽象方法
- 请在DSPPCode.mapreduce.common_pagerank.impl中创建PageRankJoinReducerImpl，继承PageRankJoinReducer，实现抽象方法
- 请在DSPPCode.mapreduce.common_pagerank.impl中创建PageRankMapperImpl，继承PageRankMapper，实现抽象方法
- 请在DSPPCode.mapreduce.common_pagerank.impl中创建PageRankReducerImpl，继承PageRankReducer，实现抽象方法

## 题目描述：

- 基于两个输入文本（网页链接关系、初始的网页排名）实现网页链接排名算法（阻尼系数以0.85计算）。
本题对网页排名值的收敛条件做了简化，如果当某一网页当前排名值与上一轮迭代排名值之间差值的绝对值小于1e-6，那么认为该网页的排名值已经收敛。
迭代停止的条件为达到最大迭代次数或某次迭代中所有网页均收敛。
网页总数N在测试阶段由后台自动给出。
* 输入格式：文本中的第一列都为网页名，列与列之间用空格分隔。其中，

  网页链接关系文本中的其他列为出站链接，如A B D表示网页A链向网页B和D（所有网页权重按1.0计算）
  ```
  A B D
  B C
  C A B
  D B C
  ```
  初始的网页排名文本第二列为该网页的排名值，如 A 1 表示网页A的排名为1
  ```
  A 1
  B 1
  C 1
  D 1
  ```

* 输出格式:
  要求分两步完成。第一步连接网页链接关系和初始的网页排名两个文件，输出连接结果：
  ```
  A 1 B D
  B 1 C
  C 1 A B
  D 1 B C
  ```
  第二步输出网页的链接关系和最终的排名值：
  ```
  A 0.21436248817266176 B D
  B 0.3633209225962085 C
  C 0.40833002013844744 A B
  D 0.1302651623462253 B C
  ```

> 注意：连接阶段无需将排名值解析为数值类型，
> 计算网页排名的阶段请将排名值解析为 `double` 类型变量进行计算。
> 输出结果的小数位数无需处理
