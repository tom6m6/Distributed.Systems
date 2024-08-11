package DSPPCode.spark.perceptron.question;

import java.io.Serializable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;


abstract public class IterationStep implements Serializable {

  /**
   * 梯度下降法修改权重的步长（学习率）
   */
  public static final double STEP = 0.01;

  /**
   * 终止条件阈值
   */
  public static final double THRESHOLD = 0.0001;


  /**
   * TODO 请完成该方法
   * <p>
   * 请在此方法中创建广播变量
   *
   * @param localVariable 本地变量
   * @return 广播变量
   */
  public abstract Broadcast<double[]> createBroadcastVariable(JavaSparkContext sc,
      double[] localVariable);

  /**
   * TODO: 终止条件，当新的权重和旧的权重平方距离（欧式距离的平方）小于阈值（0.0001）时迭代终止
   *
   * @param newWeightsAndBias 新的权重和偏置
   * @param old       上次迭代的权重
   */
  abstract public boolean termination(double[] old, double[] newWeightsAndBias);

  /**
   * TODO: 利用此类中给的工具类，根据梯度下降法求解梯度
   *
   * @param points  数据点
   * @param broadcastWeightsAndBias 权重向量和偏置
   * @return 利用梯度下降法迭代一次求出的权重向量
   */
  abstract public double[] runStep(JavaRDD<DataPoint> points, Broadcast<double[]> broadcastWeightsAndBias);

  /**
   * 迭代逻辑
   *
   * @param points  数据点
   * @param weightsAndBias 权重和偏置
   * @return 利用梯度下降法多次迭代求出的最终权重向量
   */
  protected double[] iteration(JavaSparkContext sc,JavaRDD<DataPoint> points, double[] weightsAndBias) {
    double[] old = new double[weightsAndBias.length];
    old[0] = 10;
    boolean sign = false;
    while ( !termination(old, weightsAndBias)) {
      if (sign) {
        old = weightsAndBias.clone();
      } else {
        sign = true;
      }
      Broadcast<double[]> broadcastVariable = createBroadcastVariable(sc,weightsAndBias);

      weightsAndBias = runStep(points, broadcastVariable);
    }
    return weightsAndBias;
  }

  /**
   * TODO: 实现向量求和
   * 向量求和方法，通常由reduce算子调用
   * result[i] = a[i] + b[i]
   */
  abstract public static class VectorSum implements Function2<double[], double[], double[]> {
    @Override
    abstract public double[] call(double[] a, double[] b) throws Exception;
  }

  /**
   * TODO: 根据readme所给公式求梯度
   * 根据公式计算每个点的梯度
   */
  abstract public static class ComputeGradient implements Function<DataPoint, double[]> {
    public final double[] weightsAndBias;

    public ComputeGradient(double[] weightsAndBias) {
      this.weightsAndBias = weightsAndBias;
    }

    @Override
    abstract public double[] call(DataPoint dataPoint) throws Exception;
  }

}
