package org.icgc.dcc.pcawg.client.data.sample;

import com.opencsv.bean.CsvBindByName;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.regex.Pattern;

/**
 * Use opencsv to bind (or marshal) TSVs (or CSVs that use tabs) to Beans, and can parse whole csv file into list of beans, where each bean can represent a row, and then can store List of beans in memory and do what ever aggrigations needed
 * Refer to http://opencsv.sourceforge.net/
 */
@Data
@Builder
@RequiredArgsConstructor
public class SampleBean {

  private static final Pattern pattern = Pattern.compile("^.*-US$");

  @CsvBindByName(required=true) @NonNull private String  donor_unique_id;
  @CsvBindByName(required=true) @NonNull private String  donor_wgs_exclusion_white_gray;
  @CsvBindByName(required=true) @NonNull private String  submitter_donor_id;
  @CsvBindByName(required=true) @NonNull private String  icgc_donor_id;
  @CsvBindByName(required=true) @NonNull private String  dcc_project_code;
  @CsvBindByName(required=true) @NonNull private String  aliquot_id;
  @CsvBindByName(required=true) @NonNull private String  submitter_specimen_id;
  @CsvBindByName(required=true) @NonNull private String  icgc_specimen_id;
  @CsvBindByName(required=true) @NonNull private String  submitter_sample_id;
  @CsvBindByName(required=true) @NonNull private String  icgc_sample_id;
  @CsvBindByName(required=true) @NonNull private String  dcc_specimen_type;
  @CsvBindByName(required=true) @NonNull private String  library_strategy;

  public boolean isUsProject(){
    return isUsProject(dcc_project_code);
  }

  public static boolean isUsProject(String dccProjectCode){
    return pattern.matcher(dccProjectCode).find();
  }

  public SampleBean(){}

}
