package DSPPCode.spark.perceptron.question;

import java.io.Serializable;
import java.util.Arrays;

/**
 * 数据点类
 */
public class DataPoint implements Serializable {
  public double[] x;
  public double y;

  DataPoint(double[] x, double y) {
    this.x = x;
    this.y = y;
  }

  @Override
  public String toString() {
    return "DataPoint{" +
        "x=" + Arrays.toString(x) +
        ", y=" + y +
        '}';
  }
}
