package DSPPTest.student.spark.warm_up;

import DSPPCode.spark.warm_up.impl.WordCountImpl;
import DSPPTest.student.TestTemplate;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyKV;

public class WordCountTest extends TestTemplate {

  @Test
  public void test() throws Exception {
    // 设置路径
    String inputPath = root + "/spark/warm_up/input";
    String outputPath = outputRoot + "/spark/warm_up";
    String outputFile = outputPath + "/part-00000";
    String answerFile = root + "/spark/warm_up/answer";

    // 删除旧输出
    deleteFolder(outputPath);

    // 执行
    String[] args = {inputPath, outputPath};
    WordCountImpl wordCount = new WordCountImpl();
    wordCount.run(args);

    // 检验结果
    verifyKV(readFile2String(outputFile), readFile2String(answerFile));

    System.out.println("恭喜通过~");
  }
}

