package DSPPCode.spark.knn.question;

import DSPPCode.spark.knn.impl.KNNImpl;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

public class KNNRunner {

  public static String run(String[] args) throws IOException {
    // 参数k
    int k = Integer.parseInt(args[0]);
    // 输入文件：训练数据集
    String trainFile = args[1];
    // 输入文件：查询数据集
    String queryFile = args[2];
    // 输出文件
    String outputFile = args[3];

    SparkSession ss = SparkSession.builder()
        .master("local")
        .appName("KNN")
        .getOrCreate();

    // 把训练数据转换为Data类
    JavaRDD<Data> trainData = ss.read().textFile(trainFile).javaRDD().map(line -> {
      String[] array = line.split(" ");
      int id = Integer.parseInt(array[0]);
      int y = Integer.parseInt(array[1]);
      double[] x = new double[array.length - 2];
      for (int i = 0; i < x.length; i++) {
        x[i] = Double.parseDouble(array[i + 2]);
      }
      return new Data(id, x, y);
    });

    // 把查询数据转换为Data类
    JavaRDD<Data> queryData = ss.read().textFile(queryFile).javaRDD().map(line -> {
      String[] array = line.split(" ");
      int id = Integer.parseInt(array[0]);
      double[] x = new double[array.length - 1];
      for (int i = 0; i < x.length; i++) {
        x[i] = Double.parseDouble(array[i + 1]);
      }
      return new Data(id, x);
    });

    // 写入文件
    File file = new File(outputFile);
    file.getParentFile().mkdirs();
    String rddInfo;
    try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
      JavaPairRDD<Integer, Integer> rdd = new KNNImpl(k).run(trainData, queryData);
      rddInfo = rdd.toDebugString();
      List<Tuple2<Integer, Integer>> result = rdd.collect();
      for (Tuple2<Integer, Integer> pair : result) {
        bw.write(pair._1 + "," + pair._2 + "\n");
      }
    }

    ss.stop();

    return rddInfo;
  }
}
