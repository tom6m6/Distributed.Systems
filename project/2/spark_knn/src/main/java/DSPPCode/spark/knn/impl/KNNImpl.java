package DSPPCode.spark.knn.impl;

import DSPPCode.spark.knn.question.Data;
import DSPPCode.spark.knn.question.KNN;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import java.util.ArrayList;
import java.util.List;

import java.util.*;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;

import org.apache.spark.api.java.function.PairFlatMapFunction;
public class KNNImpl extends KNN {

  public KNNImpl(int k) {
    super(k);
  }


  @Override
  public JavaPairRDD<Data, Data> kNNJoin(JavaRDD<Data> trainData, JavaRDD<Data> queryData) {
    // 将训练数据广播到所有节点
    Broadcast<List<Data>> broadcastTrainData = JavaSparkContext.fromSparkContext(trainData.context()).broadcast(trainData.collect());

    return queryData.flatMapToPair(query -> {
      List<Tuple2<Data, Data>> result = new ArrayList<>();
      for (Data train : broadcastTrainData.value()) {
        result.add(new Tuple2<>(query, train));
      }
      return result.iterator();
    });
  }

  /*
    @Override
    public JavaPairRDD<Data, Data> kNNJoin(JavaRDD<Data> trainData, JavaRDD<Data> queryData) {
      return queryData.cartesian(trainData);
    }
  */

    @Override
    public JavaPairRDD<Integer, Tuple2<Integer, Double>> calculateDistance(JavaPairRDD<Data, Data> data) {
      //data.cache();
      return data.flatMapToPair(pair -> {
        List<Tuple2<Integer, Tuple2<Integer, Double>>> distances = new ArrayList<>();
        for (int i = 0; i < pair._1.x.length; i++) {
          double distance = Math.pow(pair._1.x[i] - pair._2.x[i], 2);
          distance = Math.sqrt(distance);
          distances.add(new Tuple2<>(pair._1.id, new Tuple2<>(pair._2.y, distance)));
        }
        return distances.iterator();
      });
    }
    
    /*
  @Override
  public JavaPairRDD<Integer, Integer> classify(JavaPairRDD<Integer, Tuple2<Integer, Double>> data) {
    return data.groupByKey().mapToPair(pair -> {
      List<Tuple2<Integer, Double>> list = new ArrayList<>();
      pair._2.forEach(list::add);
      list.sort((o1, o2) -> {
        int compare = o1._2.compareTo(o2._2);
        if (compare != 0) {
          return compare;
        } else {
          return o1._1.compareTo(o2._1);
        }
      });
      return new Tuple2<>(pair._1, list.get(0)._1);
    });
  }
*/
  @Override
  public JavaPairRDD<Integer, Integer> classify(JavaPairRDD<Integer, Tuple2<Integer, Double>> data) {
    final ClassTag<Integer> classTagInteger = ClassTag$.MODULE$.apply(Integer.class);
    final Broadcast<Integer> broadcastK = data.context().broadcast(k, classTagInteger);
    return data.mapToPair(pair -> new Tuple2<>(new Tuple2<>(pair._1, pair._2._1), pair._2._2))
        .combineByKey((Double value) -> new Tuple2<>(1, value),
            (Tuple2<Integer, Double> tuple, Double value) -> new Tuple2<>(tuple._1 + 1, Math.min(tuple._2, value)),
            (Tuple2<Integer, Double> tuple1, Tuple2<Integer, Double> tuple2) -> new Tuple2<>(tuple1._1 + tuple2._1, Math.min(tuple1._2, tuple2._2))
        )
       .mapPartitionsToPair((PairFlatMapFunction<Iterator<Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double>>>, Integer, Tuple2<Integer, Double>>) iterator -> {
         Map<Integer, PriorityQueue<Tuple2<Integer, Double>>> map = new HashMap<>();
         while (iterator.hasNext()) {
           Tuple2<Tuple2<Integer, Integer>, Tuple2<Integer, Double>> tuple = iterator.next();
           PriorityQueue<Tuple2<Integer, Double>> queue = map.getOrDefault(tuple._1._1, new PriorityQueue<>(Comparator.comparing(Tuple2::_2)));
           if (queue.size() < broadcastK.value()) {
             queue.add(new Tuple2<>(tuple._1._2, tuple._2._2));
           } else if (queue.peek()._2 < tuple._2._2) {
             queue.poll();
             queue.add(new Tuple2<>(tuple._1._2, tuple._2._2));
           }
           map.put(tuple._1._1, queue);
         }
         List<Tuple2<Integer, Tuple2<Integer, Double>>> result = new ArrayList<>();
         for (Map.Entry<Integer, PriorityQueue<Tuple2<Integer, Double>>> entry : map.entrySet()) {
           PriorityQueue<Tuple2<Integer, Double>> queue = entry.getValue();
           while (!queue.isEmpty()) {
             result.add(new Tuple2<>(entry.getKey(), queue.poll()));
           }
         }
         return result.iterator();
       }).reduceByKey((tuple1, tuple2) -> tuple1._2 < tuple2._2 ? tuple1 : tuple2)
       .mapToPair(pair -> {
         Map<Integer, Double> map = new HashMap<>();
         map.put(pair._2._1, map.getOrDefault(pair._2._1, 0.0) + 1 / pair._2._2);
         int maxLabel = Collections.max(map.entrySet(), Comparator.comparing(Map.Entry::getValue)).getKey();
         return new Tuple2<>(pair._1, maxLabel);
       });
  }
}

