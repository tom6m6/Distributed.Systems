package DSPPCode.spark.connected_components.impl;

import DSPPCode.spark.connected_components.question.ConnectedComponents;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class ConnectedComponentsImpl extends ConnectedComponents {
  @Override
  public JavaPairRDD<String, Integer> getcc(JavaRDD<String> text) {
// 将输入的文本转换为边的形式
    JavaPairRDD<String, Integer> edges = text.flatMapToPair(line -> {
      String[] vertices = line.split("\t");
      List<Tuple2<String, Integer>> pairs = new ArrayList<>();
      for (int i = 1; i < vertices.length; i++) {
        pairs.add(new Tuple2<>(vertices[0], Integer.parseInt(vertices[i])));
        pairs.add(new Tuple2<>(vertices[i], Integer.parseInt(vertices[0])));
      }
      //我让每个点到自己都有自环
      pairs.add(new Tuple2<>(vertices[0], Integer.parseInt(vertices[0])));


      return pairs.iterator();
    }).distinct();

// 初始化每个顶点的最小顶点ID为其自身
    JavaPairRDD<String, Integer> minVertex = text.flatMapToPair(line -> {
      String[] vertices = line.split("\t");
      List<Tuple2<String, Integer>> pairs = new ArrayList<>();
      for (String vertex : vertices) {
        pairs.add(new Tuple2<>(vertex, Integer.parseInt(vertex)));
      }
      return pairs.iterator();
    }).reduceByKey(Math::min);

    while (true) {
// 计算新的最小顶点ID
      JavaPairRDD<String, Integer> newMinVertex = edges.join(minVertex)
          .values()
          .mapToPair(tuple -> new Tuple2<>(tuple._1.toString(), Math.min(tuple._1, tuple._2)))
          .reduceByKey(Math::min);

// 如果最小顶点ID没有发生改变，那么结束迭代
      if (!isChange(minVertex, newMinVertex)) {
        break;
      }

      minVertex = newMinVertex;
    }

    return minVertex;
  }
}