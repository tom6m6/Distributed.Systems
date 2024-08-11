package DSPPTest.student.spark.connected_components;

import DSPPCode.spark.connected_components.impl.ConnectedComponentsImpl;
import DSPPCode.spark.connected_components.question.ConnectedComponents;
import DSPPTest.student.TestTemplate;
import org.junit.Before;
import org.junit.Test;

import static DSPPTest.util.FileOperator.deleteFolder;
import static DSPPTest.util.FileOperator.readFile2String;
import static DSPPTest.util.Verifier.verifyKV;

public class ConnectedComponentsTest extends TestTemplate {

  // 设置路径
  private static final String BASE_INPUT_PATH = root + "/spark/connected_components/input";
  private static final String OUTPUT_PATH = outputRoot + "/spark/connected_components";
  private static final String OUTPUT_FILE = OUTPUT_PATH + "/part-00000";
  private static final String BASE_ANSWER_FILE = root + "/spark/connected_components/answer";

  @Before
  public void deleteOutput() {
    // 删除旧输出
    deleteFolder(OUTPUT_PATH);
  }

  @Test
  public void test() throws Exception {

    // 执行
    String[] args = {BASE_INPUT_PATH, OUTPUT_PATH};
    ConnectedComponents connectedComponents = new ConnectedComponentsImpl();
    connectedComponents.run(args);

    System.out.println(readFile2String(OUTPUT_FILE));
    // 检验结果
    verifyKV(readFile2String(OUTPUT_FILE), readFile2String(BASE_ANSWER_FILE));

    System.out.println("恭喜通过~");
  }

}
