package DSPPCode.spark.connected_components.question;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.codehaus.janino.Java;
import java.io.Serializable;
import scala.Tuple2;

public abstract class ConnectedComponents implements Serializable {

  private static final String MODE = "local";

  public void run(String[] args) {
    JavaSparkContext sc = new JavaSparkContext(MODE, getClass().getName());
    // 读入文本数据，创建名为xt的RDD
    JavaRDD<String> text = sc.textFile(args[0]);
    JavaPairRDD<String, Integer> cc = getcc(text);
    // 输出结果到文本文件中
    cc.saveAsTextFile(args[1]);
    sc.close();
  }

  /**
   * TODO 请完成该方法
   * <p>
   * 请在此方法中计算各个连通分量的最小顶点ID
   *
   * @param text 包含了输入文本文件数据的RDD，提供了无向图的边关系
   * @return [顶点ID-所属连通分量中最小的顶点ID]键值对
   */

  public abstract JavaPairRDD<String, Integer> getcc(JavaRDD<String> text);


  /**
   * 通过判断各连通分量最小顶点ID迭代前后是否发生改变 判断是否需要终止迭代
   */
  static public boolean isChange(JavaPairRDD<String, Integer> min, JavaPairRDD<String, Integer> min_new) {
    // 长度是否相同
    long count1,count2,count;
    count1 = min.count();
    count2 = min_new.count();
    if (count1 != count2) { return false; } else { count = count1; }

    // 比较迭代前后迭代后是否变化
    long changed_count =
        min.join(min_new).filter(
            new Function<Tuple2<String, Tuple2<Integer, Integer>>, Boolean>() {
              @Override
              public Boolean call(Tuple2<String, Tuple2<Integer, Integer>> line) throws Exception {
                if(line._2._1.equals(line._2._2)){
                  return true;
                }
                return false;
              }
            }
        ).count();
    if(changed_count == count) { return false;}
    else { return true; }
  }
}
