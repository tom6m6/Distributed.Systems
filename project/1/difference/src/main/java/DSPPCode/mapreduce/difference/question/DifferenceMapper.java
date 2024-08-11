package DSPPCode.mapreduce.difference.question;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

abstract public class DifferenceMapper extends Mapper<Object, Text, Text, Text> {
  /**
   * TODO 请完成该抽象方法
   * -
   * 输入：
   * <p>
   * 输入有两个文件，文本中的第一列均为为学生姓名，第二列均为图书ID，列与列之间通过Tab分隔（没有表头）
   * e.g. Alice	1
   */
    abstract public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException;
}



