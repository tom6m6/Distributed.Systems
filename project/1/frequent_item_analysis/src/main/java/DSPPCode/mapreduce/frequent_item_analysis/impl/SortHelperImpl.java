package DSPPCode.mapreduce.frequent_item_analysis.impl;

import DSPPCode.mapreduce.frequent_item_analysis.question.SortHelper;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class SortHelperImpl extends SortHelper {

  @Override
  public List<String> sortSeq(List<String> input) {
    input.sort(new Comparator<String>() {
      @Override
      public int compare(String o1, String o2) {
        return o1.compareTo(o2);
      }
    });
    return input;
  }
}
