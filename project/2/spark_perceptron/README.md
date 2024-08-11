# perceptron

## 难易程度： **难

## 待完成
* 请在DSPPCode.spark.perceptron.impl中创建IterationStepImpl，继承IterationStep，实现抽象方法



## 问题描述
单层感知机模型通过拟合一个线性函数，再通过阶跃函数输出 {-1, +1} 的类别标签，直接通过标签来判断因变量的类别，其中$x \in R^n$为输入，$w \in R ^ n$为权重，$b \in R^n$为偏置：
$$
f(x) = \begin{cases}
-1, & \text{if } w \cdot x + b < 0 \\
+1, & \text{if } w \cdot x + b \geq 0
\end{cases}
$$

我们使用经验损失函数作为代价函数，其中$M$为错分类样本集合：
$$
L(w,b) = - \sum_{x_i \in M} {y_i(w\cdot x_i + b)}
$$

随后我们使用梯度下降法求解新的权重$w$和偏置$b$，其中$\eta$为学习率：
$$w^{t+1} \leftarrow w^{t} + \eta (y - f({x_i})) x_i$$
$$b^{t+1} \leftarrow b^{t} + \eta (y - f({x_i}))$$


要求：使用梯度下降算法求解单层感知机模型参数。

* 输入格式

```
-1 1 0
-1 2 0
-1 2 1
+1 0 1
+1 0 2
+1 0 9
+1 1 2
+1 1 3
```
第一列表示数据的类别 +1 或 -1，第二列到最后一列表示数据不同维度上的值，数据类型均为 double。即 <y, x1, x2, ..., xn>，数据维度为 n。

* 输出格式
```
w0,-0.07
w1,0.14
b,0.00
```
第一列为模型参数下标，第二列为对应下标的模型参数值。上面展示维度为 2 的数据经计算后的参数结果。
