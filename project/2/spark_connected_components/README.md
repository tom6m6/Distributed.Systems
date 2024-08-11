# Connected Components

## 难易程度:  **中

## 待完成

- 请在 DSPPCode.spark.connected_components.impl 中创建 ConnectedComponentsImpl, 继承 ConnectedComponents, 实现抽象方法

## 题目描述

- 根据输入文本中的顶点关系，找出无向图中每个连通分量的最小顶点 ID。

- 输入格式：第一列表示顶点 ID，第二列起为与第一列顶点相连的顶点 ID (分隔符为tab)。例如，第一行表示顶点1与顶点2,顶点3直接连接。

    ```
    1	2	3
    2	1
    3	1
    4	6
    5	6
    6	4	5
    ```
    
- 输出格式：第一列输出顶点 ID，第二列表示该连通分量中最小的顶点 ID。例如，第一行表示在顶点1的连通分量中最小的顶点ID是1。

    ```
    (1,1)
    (2,1)
    (3,1)
    (4,4)
    (5,4)
    (6,4)
    ```

    
