package DSPPCode.spark.perceptron.question;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.regex.Pattern;
import DSPPCode.spark.perceptron.impl.IterationStepImpl;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

/**
 * 梯度下降法求解单层感知机模型
 */
public class PerceptronRunner {
  private static final DecimalFormat FORMAT = new DecimalFormat("#0.00");
  /**
   * 数据维度
   */
  private static int D;

  /**
   * 解析字符串生成DataPoint
   * 字符串[1 1 2 3 4 5 6 7 8 9 10]
   * y = 1
   * x = [1 2 3 4 5 6 7 8 9 10]
   */
  public static class ParsePoint implements Function<String, DataPoint> {
    private static final Pattern SPACE = Pattern.compile(" ");

    @Override
    public DataPoint call(String line) throws Exception {
      String[] tok = SPACE.split(line);
      double y = Double.parseDouble(tok[0]);
      double[] x = new double[D];
      for (int i = 0; i < D; i++) {
        x[i] = Double.parseDouble(tok[i + 1]);
      }
      return new DataPoint(x, y);
    }
  }

  public static int run(String[] args) throws IOException {
    D = Integer.parseInt(args[2]);
    SparkSession spark = SparkSession
        .builder()
        .master("local")
        .appName("LogisticRegression")
        .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();
    JavaRDD<DataPoint> points = lines.map(new ParsePoint()).cache();
    // 初始化权重W和偏置默认是0
    double[] weightAndBias = new double[D+1];

    // 梯度下降法求解
    weightAndBias = new IterationStepImpl().iteration(JavaSparkContext.fromSparkContext(spark.sparkContext()),points, weightAndBias);
    BufferedWriter bw = new BufferedWriter(new FileWriter(new File(args[1])));

    for (int i = 0; i < weightAndBias.length-1; i++) {
      bw.write("w" + i + "," + FORMAT.format(weightAndBias[i]) + "\n");
    }
    bw.write("b" + "," + FORMAT.format(weightAndBias[weightAndBias.length-1]) + "\n");
    bw.close();
    spark.stop();

    return 0;
  }
}
