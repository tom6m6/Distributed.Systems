# Transitive Closure

## 难易程度： **中

## 待完成
* 请在 DSPPCode.mapreduce.transitive_closure.impl 中创建 TransitiveClosureMapperImpl, 继承TransitiveClosureMapper, 实现抽象方法。
* 请在 DSPPCode.mapreduce.transitive_closure.impl 中创建 TransitiveClosureReducerImpl, 继承TransitiveClosureReducer, 实现抽象方法。
 
## 题目描述:

* 在数学中，集合$X$上的二元关系$R$的传递闭包指的是包含$R$的$X$上的最小的传递关系，记作$t(R)$。例如，假设集合$X$为人的集合{a,b,c}，二元关系$R$为父子关系{<a,b>,<b,c>}，其中<a,b>和<b,c>分别表示a是b的父亲以及b是c的父亲，则$t(R)$应为祖宗-后代关系{<a,b>,<b,c>,<a,c>}。当前，社保局拿到了一份名单，该名单给出了子女-父母的关系。社保局想要从该名单中分析出哪些人之间为孙子女-祖父母的关系。然而，名单很庞大，如果手工分析可能需要几个小时，于是社保局希望你能帮忙编写程序自动地进行分析。

* 输入格式:

  ```
  child parent
  Jack Philip
  Jack Jesse
  Philip Terry
  Philip Alma
  ```

  输入保存在文本中，文本的第一行为关系标识，其余行为子女和父母的对应关系。以上述示例为例，`Jack Philip`表示Jack与Philip之间存在着子女-父母的关系。

* 输出格式:

  ```
  Jack Terry
  Jack Alma
  ```

  输出保存在文本中，文本的每一行为孙子女和祖父母的对应关系。以上述示例为例，`Jack,Terry`表示Jack与Terry之间存在着孙子女-祖父母关系。