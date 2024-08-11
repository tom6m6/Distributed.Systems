package DSPPCode.spark.knn.question;

import java.io.Serializable;

/**
 * 数据类
 */
public class Data implements Serializable {
  public int id;
  public double[] x;
  public int y;

  public Data(int id, double[] x, int y) {
    this.id = id;
    this.x = x;
    this.y = y;
  }

  public Data(int id, double[] x) {
    this.id = id;
    this.x = x;
    this.y = -1;
  }
}
