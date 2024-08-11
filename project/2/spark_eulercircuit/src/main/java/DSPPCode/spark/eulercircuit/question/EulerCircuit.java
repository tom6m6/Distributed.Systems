package DSPPCode.spark.eulercircuit.question;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.ArrayList;
import java.util.List;

public abstract class EulerCircuit {

  private static final String MODE = "local";

  public void run(String[] args) {
    // 创建JavaSparkContext
    JavaSparkContext jsc = new JavaSparkContext(MODE, getClass().getName());
    // 读入文本数据，创建名为lines的RDD
    JavaRDD<String> lines = jsc.textFile(args[0]).cache();

    // 判断是否是欧拉回路，是返回true，不是返回false
    boolean ans = isEulerCircuit(lines,jsc);

    // 将结果写入文本文件中
    List list = new ArrayList<String>();
    list.add(ans ? "Yes" : "No");
    JavaRDD res = jsc.parallelize(list);
    res.saveAsTextFile(args[1]);

    jsc.close();
  }

  public abstract boolean isEulerCircuit(JavaRDD<String> lines,JavaSparkContext jsc);
}
