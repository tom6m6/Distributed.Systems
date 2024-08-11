# Difference

## 难易程度： * 易

## 待完成

- 请在DSPPCode.mapreduce.difference.impl中创建DifferenceMapperImpl, 继承DifferenceMapper, 实现抽象方法。
- 请在DSPPCode.mapreduce.difference.impl中创建DifferenceReducerImpl, 继承DifferenceReducer, 实现抽象方法。

## 题目描述

- 学校图书馆的自助借书系统因为网络原因崩溃了，无法同步更新每天借书的学生信息，两位图书管理员只能各自记录了一份借阅图书（R）和归还图书（S）的学生名单。现在，他们希望你能够帮助他们合并两份名单，整理为一份**还未归还图书**的学生名单。 
  请你通过MapReduce程序来实现以上功能。

- 输入格式：输入只有一个文件，文本的第一列为学生姓名，第二列为图书ID，列与列之间用Tab分隔。
  - R:
    ```
    Alice	1  
    Bob	2  
    Sam	3  
    Era	4 
    ```
  - S:  
    ```
    Alice	1  
    Bob	2   
    ```

- 输出格式：与输入文件的格式相同
  ```
  Era	4  
  Sam	3   
  ```
