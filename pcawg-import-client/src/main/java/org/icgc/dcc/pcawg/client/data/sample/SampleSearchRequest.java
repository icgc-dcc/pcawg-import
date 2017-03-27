package org.icgc.dcc.pcawg.client.data.sample;

import com.opencsv.bean.CsvBindByName;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Data
@Builder
public class SampleSearchRequest {

  @CsvBindByName(required=true) @NonNull private String  donor_unique_id;
  @CsvBindByName(required=true) @NonNull private String  library_strategy;
  @CsvBindByName(required=true) @NonNull private String  dcc_specimen_type;
  @CsvBindByName(required=true) @NonNull private String  aliquot_id;

}
