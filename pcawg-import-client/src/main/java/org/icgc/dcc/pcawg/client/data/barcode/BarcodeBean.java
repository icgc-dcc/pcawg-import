package org.icgc.dcc.pcawg.client.data.barcode;

import com.opencsv.bean.CsvBindByName;
import lombok.Data;
import lombok.NonNull;

@Data
public class BarcodeBean {

  @CsvBindByName(required = true, column = "#project")
  @NonNull private String project;

  @CsvBindByName(required = true) @NonNull private String entity_type;
  @CsvBindByName(required = true) @NonNull private String uuid;
  @CsvBindByName(required = true) @NonNull private String barcode;

  // Uuids should be lowercase
  public void setUuid(String uuid){
    this.uuid = uuid.toLowerCase();
  }

  public BarcodeBean() { }
}
