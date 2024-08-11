package DSPPCode.spark.knn.question;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import java.io.Serializable;

public abstract class KNN implements Serializable {

  /**
   * 参数k
   */
  public final int k;

  public KNN(int k) {
    this.k = k;
  }

  /**
   * TODO: KNN Join
   *
   * @param trainData 训练数据集
   * @param queryData 查询数据集
   * @return KNN join的结果
   */
  public abstract JavaPairRDD<Data, Data> kNNJoin(JavaRDD<Data> trainData, JavaRDD<Data> queryData);

  /**
   * TODO: 计算查询数据与训练数据之间的欧式距离
   *
   * @param data KNN join结果
   * @return 查询数据与训练数据之间的距离
   */
  public abstract JavaPairRDD<Integer, Tuple2<Integer, Double>> calculateDistance(JavaPairRDD<Data, Data> data);

  /**
   * TODO: 分类
   *
   * @param data 查询数据与训练数据之间的距离
   * @return 查询数据集分类结果
   */
  public abstract JavaPairRDD<Integer, Integer> classify(JavaPairRDD<Integer, Tuple2<Integer, Double>> data);

  /**
   * KNN运行流程
   *
   * @param trainData 训练数据集
   * @param queryData 查询数据集
   * @return 查询数据集分类结果
   */
  public final JavaPairRDD<Integer, Integer> run(JavaRDD<Data> trainData, JavaRDD<Data> queryData) {
    return classify(calculateDistance(kNNJoin(trainData, queryData)));
  }
}
