package org.icgc.dcc.pcawg.client.tsv;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

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


  @SneakyThrows
  public void analyze(){
    reset();
    val file = new File(tsvFilename);
    val br = new BufferedReader(new FileReader(file));
    br.lines()
        .forEach(this::checkLine);
  }

  private void reset(){
    illegalStringsCount = 0;
    numBadColumnCount = 0;
    numNullsCount = 0;
  }

  private void checkLine(String line){
    checkNull(line);
    checkColumnSize(line);
    checkIllegalStrings(line);
  }

  private void checkNull(String line){
    if (line == null){
      numNullsCount++;
    }
  }

  private void checkColumnSize(String line){
    val actualNumTabs = StringUtils.countMatches(line, TAB_STRING);
    val expectedNumTabs = this.expectedNumColumns - 1;
    if( actualNumTabs != expectedNumTabs){
      numBadColumnCount++;
    }
  }

  private void checkIllegalStrings(String line){
    if(line.matches(ILLEGAL_STRING_REGEX)){
      illegalStringsCount++;
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
        + str("numIllegalStringCount: %s", illegalStringsCount);
  }

  public void log(){
    if (isOk()) {
      log.info("[TSV Validation PASSED]: {}", toString());
    } else {
      log.error("[TSV Validation FAILED]: {}", toString());
    }
  }

}
