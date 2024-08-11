package DSPPTest.student.spark.knn;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyKV;

import DSPPCode.spark.knn.question.KNNRunner;
import DSPPTest.student.TestTemplate;
import org.junit.Test;

public class KNNTest extends TestTemplate {

  @Test(timeout = 30000)
  public void test1() throws Exception {
    String trainFile = root + "/spark/knn/input/train";
    String queryFile = root + "/spark/knn/input/query";
    String outputFile = outputRoot + "/spark/knn/output";
    String answer = root + "/spark/knn/answer";

    // 删除旧输出
    deleteFolder(outputFile);

    String[] args = {"2", trainFile, queryFile, outputFile};
    KNNRunner.run(args);

    verifyKV(readFile2String(outputFile), readFile2String(answer));
    System.out.println("恭喜通过~");
  }
}
