package DSPPCode.mapreduce.frequent_item_analysis.question;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

abstract public class FrequentItemAnalysisMapper extends
    Mapper<LongWritable, Text,Text, IntWritable> {
  /**
   * TODO 请完成该抽象方法
   * -
   * 输入：
   * 交易记录表，每一行代表一次交易记录，一次交易记录中的商品使用英语逗号","隔开
   */
  abstract public void map(LongWritable key,Text value,Context context)
      throws IOException, InterruptedException;

}
