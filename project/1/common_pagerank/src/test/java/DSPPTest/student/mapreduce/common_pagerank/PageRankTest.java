package DSPPTest.student.mapreduce.common_pagerank;

import DSPPCode.mapreduce.common_pagerank.question.PageRankJoinRunner;
import DSPPCode.mapreduce.common_pagerank.question.PageRankRunner;
import DSPPTest.student.TestTemplate;
import DSPPTest.util.Parser.KVParser;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyKV;

public class PageRankTest extends TestTemplate {

  @Test
  public void test() throws Exception {
    // 设置路径
    String inputPath = root + "/mapreduce/common_pagerank/input";
    String outputPath = outputRoot + "/mapreduce/common_pagerank";

    String outputPath1 = outputPath + "/step_one";
    String outputFile1 = outputPath1 + "/part-r-00000";
    String outputPath2 = outputPath + "/step_two/";
    String outputFile2 = outputPath2 + "19/part-r-00000";

    String answerFile1 = root + "/mapreduce/common_pagerank/answer-step1";
    String answerFile2 = root + "/mapreduce/common_pagerank/answer-step2";

    // 删除旧的输出
    deleteFolder(outputPath);

    // 执行
    String[] args1 = {inputPath, outputPath1};
    ToolRunner.run(new PageRankJoinRunner(), args1);
    String[] args2 = {outputFile1, outputPath2, "4"};
    PageRankRunner pageRank = new PageRankRunner();
    pageRank.mainRun(args2);

    // 检验结果
    try {
      verifyKV(readFile2String(outputFile1), readFile2String(answerFile1), new KVParser(" "));
      System.out.println("Join 结果正确");
    } catch (Throwable t) {
      System.out.println("Join 结果错误.");
    }
    try {
      verifyKV(readFile2String(outputFile2), readFile2String(answerFile2), new KVParser(" "));
      System.out.println("PageRank 结果正确");
    } catch (Throwable t) {
      System.out.println("PageRank 结果错误.");
    }
  }
}
