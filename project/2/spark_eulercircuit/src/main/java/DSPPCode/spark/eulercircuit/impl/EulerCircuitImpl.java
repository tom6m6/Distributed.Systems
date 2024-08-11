package DSPPCode.spark.eulercircuit.impl;

import DSPPCode.spark.eulercircuit.question.EulerCircuit;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class EulerCircuitImpl extends EulerCircuit {

  @Override
  public boolean isEulerCircuit(JavaRDD<String> lines, JavaSparkContext jsc) {
    // 将输入的每一行转换为(a, b)的形式
    JavaPairRDD<Integer, Integer> edges = lines.mapToPair(line -> {
      String[] nodes = line.split(" ");
      return new Tuple2<>(Integer.parseInt(nodes[0]), Integer.parseInt(nodes[1]));
    });

    // 计算每个节点的度数
    JavaPairRDD<Integer, Integer> degrees = edges.flatMapToPair(edge -> {
      List<Tuple2<Integer, Integer>> list = new ArrayList<>();
      list.add(new Tuple2<>(edge._1, 1));
      list.add(new Tuple2<>(edge._2, 1));
      return list.iterator();
    }).reduceByKey(Integer::sum);

    // 检查所有节点的度数是否都是偶数
    long oddDegreeNodes = degrees.filter(degree -> degree._2 % 2 != 0).count();

    // 如果所有节点的度数都是偶数，那么存在欧拉回路
    return oddDegreeNodes == 0;
  }
}
