package org.icgc.dcc.pcawg.client.model.metadata.project;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.val;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Use opencsv to bind (or marshal) TSVs (or CSVs that use tabs) to Beans, and can parse whole csv file into list of beans, where each bean can represent a row, and then can store List of beans in memory and do what ever aggrigations needed
 * Refer to http://opencsv.sourceforge.net/
 */
@Value
@Builder
public class SampleSheetModel {
  private static final int ICGC_DONOR_ID = 5;
  private static final int DONOR_UNIQUE_ID_POS = 0;
  private static final int SUBMITTER_SPECIMEN_ID_POS = 8;
  private static final int DCC_SPECIMEN_TYPE = 10;
  private static final int LIBRARY_STRATEGY = 11;
  private static final int DCC_PROJECT_CODE= 4;
  private static final int MAX_NUM_COLUMNS = 12;

  @NonNull
  private final String donorUniqueId;

  @NonNull
  private final String aliquot_id;

  @NonNull
  private final String submitterSampleId;

  @NonNull
  private final String dccSpecimenType;

  @NonNull
  private final String libraryStrategy;

  @NonNull
  private final String dccProjectCode;

  public static SampleSheetModel newSampleSheetModel(String tsvLine){

    val array = tsvLine.trim().split("\t");
    checkArgument(array.length == MAX_NUM_COLUMNS, "Max allowed columns is %s, but input columns is %s", MAX_NUM_COLUMNS, array.length);
    return SampleSheetModel.builder()
        .aliquot_id(array[ICGC_DONOR_ID])
        .dccSpecimenType(array[DCC_SPECIMEN_TYPE])
        .donorUniqueId(array[DONOR_UNIQUE_ID_POS])
        .dccProjectCode(array[DCC_PROJECT_CODE])
        .libraryStrategy(array[LIBRARY_STRATEGY])
        .submitterSampleId(array[SUBMITTER_SPECIMEN_ID_POS])
        .build();
  }

}
