package org.icgc.dcc.pcawg.client.data.sample;

import lombok.NoArgsConstructor;
import lombok.val;

import static com.google.common.base.Preconditions.checkArgument;
import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class SampleSheetParser {
  private static final int DONOR_UNIQUE_ID_POS = 0;
  private static final int DONOR_WGS_EXCLUSION_WHITE_GRAY_POS = 1;
  private static final int SUBMITTER_DONOR_ID_POS = 2;
  private static final int ICGC_DONOR_ID_POS = 3;
  private static final int DCC_PROJECT_CODE_POS = 4;
  private static final int ALIQUOT_ID_POS = 5;
  private static final int SUBMITTER_SPECIMEN_ID_POS = 6;
  private static final int ICGC_SPECIMEN_ID_POS = 7;
  private static final int SUBMITTER_SAMPLE_ID_POS = 8;
  private static final int ICGC_SAMPLE_ID_POS = 9;
  private static final int DCC_SPECIMEN_TYPE_POS = 10;
  private static final int LIBRARY_STRATEGY_POS = 11;
  private static final int MAX_NUM_COLUMNS = 12;
  private static final String TAB = "\t";

  public static SampleBean parseLine(String tsvLine){
    val a = tsvLine.trim().split(TAB);
    checkArgument(a.length == MAX_NUM_COLUMNS, "Max allowed columns is %s, but input columns is %s", MAX_NUM_COLUMNS, a.length);
    val b = new SampleBean();
    b.setSubmitter_specimen_id(a[SUBMITTER_SPECIMEN_ID_POS]);
    b.setSubmitter_sample_id(a[SUBMITTER_SAMPLE_ID_POS]);
    b.setSubmitter_donor_id(a[SUBMITTER_DONOR_ID_POS]);
    b.setLibrary_strategy(a[LIBRARY_STRATEGY_POS]);
    b.setIcgc_specimen_id(a[ICGC_SPECIMEN_ID_POS]);
    b.setIcgc_sample_id(a[ICGC_SAMPLE_ID_POS]);
    b.setIcgc_donor_id(a[ICGC_DONOR_ID_POS]);
    b.setDonor_wgs_exclusion_white_gray(a[DONOR_WGS_EXCLUSION_WHITE_GRAY_POS]);
    b.setDonor_unique_id(a[DONOR_UNIQUE_ID_POS]);
    b.setDcc_specimen_type(a[DCC_SPECIMEN_TYPE_POS]);
    b.setDcc_project_code(a[DCC_PROJECT_CODE_POS]);
    b.setAliquot_id(a[ALIQUOT_ID_POS]);
    return b;
  }



}
