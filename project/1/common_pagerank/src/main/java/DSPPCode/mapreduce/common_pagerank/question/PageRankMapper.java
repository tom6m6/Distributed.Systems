package DSPPCode.mapreduce.common_pagerank.question;

import DSPPCode.mapreduce.common_pagerank.question.utils.ReducePageRankWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 第二步的 Mapper 已经给出，请完成与之配合的 Reducer
 */
public class PageRankMapper extends Mapper<LongWritable, Text, Text, ReducePageRankWritable> {
  /**
   * 输入：网页链接关系和网页排名连接后的结果
   * <p>
   * 如 A 1.0 B D 表示网页A的排名为1，并且链向网页B和D (题目中网页权重均按1.0计算)
   * <p>
   * 可借助ReducePageRankWritable类来实现
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    // 以空格为分隔符切分
    String[] pageInfo = value.toString().split(" ");
    // 网页的排名值
    double pageRank = Double.parseDouble(pageInfo[1]);
    // 网页的出站链接数
    int outLink = pageInfo.length - 2;

    ReducePageRankWritable writable;
    writable = new ReducePageRankWritable();
    // 计算贡献值并保存
    writable.setData(String.valueOf(pageRank / outLink));
    // 设置对应标识
    writable.setTag(ReducePageRankWritable.PR_L);
    // 对于每一个出站链接，输出贡献值
    for (int i = 2; i < pageInfo.length; i++) {
      context.write(new Text(pageInfo[i]), writable);
    }
    writable = new ReducePageRankWritable();
    // 保存网页信息并标识
    writable.setData(value.toString());
    writable.setTag(ReducePageRankWritable.PAGE_INFO);
    // 以输入的网页信息的网页名称为key进行输出
    context.write(new Text(pageInfo[0]), writable);
  }
}