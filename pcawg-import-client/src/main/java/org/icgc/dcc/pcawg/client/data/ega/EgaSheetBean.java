package org.icgc.dcc.pcawg.client.data.ega;

import com.opencsv.bean.CsvBindByName;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

@Data
public class EgaSheetBean {

  private static int INSTANCE_COUNT = 0;

  @CsvBindByName(required = true) @NonNull private String dataset_id;
  @CsvBindByName(required = true) @NonNull private String analysis_id;
  @CsvBindByName(required = true) @NonNull private String file_id;
  @CsvBindByName(required = true) @NonNull private String file;
  @CsvBindByName(required = true) @NonNull private String sample_id;
  @CsvBindByName(required = true) @NonNull private String submitter_sample_id;
  @CsvBindByName(required = true) @NonNull private String icgc_sample_id;
  @CsvBindByName(required = true) @NonNull private String aliquot_id;
  @CsvBindByName(required = true) @NonNull private String icgc_project_code;

  @Getter
  private final int id;

  public EgaSheetBean(){
    this.id = ++INSTANCE_COUNT;
  }

}
