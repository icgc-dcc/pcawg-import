package org.icgc.dcc.pcawg.client.tsv;

import com.google.common.collect.Sets;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Set;

import static java.util.stream.Collectors.joining;
import static lombok.AccessLevel.PRIVATE;

@Slf4j
@RequiredArgsConstructor(access = PRIVATE)
public class TsvValidator {
  private static final String ILLEGAL_STRING_REGEX = "^\\s+$";
  private static final String TAB_STRING = "\t";

  public static TsvValidator newTsvValidator(String tsvFilename, final int expectedNumColumns){
    return new TsvValidator(tsvFilename, expectedNumColumns);
  }

  private final String tsvFilename;
  private final int expectedNumColumns;

  /**
   * State
   */
  private int illegalStringsCount;
  private int numNullsCount;
  private int numBadColumnCount;
  private int lineCount;
  private Set<Integer> badLineNumbers;


  @SneakyThrows
  public void analyze(){
    reset();
    val file = new File(tsvFilename);
    val br = new BufferedReader(new FileReader(file));
    br.lines()
        .forEach(this::checkLine);
  }

  private void reset(){
    lineCount = 0;
    badLineNumbers = Sets.newTreeSet();
    illegalStringsCount = 0;
    numBadColumnCount = 0;
    numNullsCount = 0;
  }

  private void checkLine(String line){
    ++lineCount;
    checkNull(line, lineCount);
    checkColumnSize(line, lineCount);
    checkIllegalStrings(line, lineCount);
  }

  private void checkNull(String line, int lineNum){
    if (line == null){
      numNullsCount++;
      badLineNumbers.add(lineNum);
    }
  }

  private void checkColumnSize(String line, int lineNum){
    val actualNumTabs = StringUtils.countMatches(line, TAB_STRING);
    val expectedNumTabs = this.expectedNumColumns - 1;
    if( actualNumTabs != expectedNumTabs){
      numBadColumnCount++;
      badLineNumbers.add(lineNum);
    }
  }

  private void checkIllegalStrings(String line, int lineNum){
    if(line.matches(ILLEGAL_STRING_REGEX)){
      illegalStringsCount++;
      badLineNumbers.add(lineNum);
    }
  }

  public boolean isFreeOfNulls(){
    return numNullsCount == 0;
  }

  public boolean isColumnNumberOk(){
    return numBadColumnCount == 0;
  }

  public boolean isFreeOfIllegalStrings(){
    return illegalStringsCount == 0;
  }

  public boolean isOk(){
    return isFreeOfIllegalStrings() && isColumnNumberOk() && isFreeOfNulls();
  }

  private static String str(String s, Object ... objects){
    return String.format(s, objects);
  }

  public String toString(){
    return ""
        + str("Filename: %s, ", tsvFilename)
        + str("isFreeOfNulls: %s, ", isFreeOfNulls())
        + str("isColumnNumberOk: %s, ", isColumnNumberOk())
        + str("isFreeOfIllegalStrings: %s, ", isFreeOfIllegalStrings())
        + str("isOk: %s, ", isOk())
        + str("numNullCount: %s, ", numNullsCount)
        + str("numBadColumnCount: %s, ", numBadColumnCount)
        + str("numIllegalStringCount: %s, ", illegalStringsCount)
        + str("badLineNumbers: %s", badLineNumbers.stream()
        .map(Object::toString)
        .collect(joining(";")));
  }

  public void log(){
    if (isOk()) {
      log.info("[TSV Validation PASSED]: {}", toString());
    } else {
      log.error("[TSV Validation FAILED]: {}", toString());
    }
  }

}
