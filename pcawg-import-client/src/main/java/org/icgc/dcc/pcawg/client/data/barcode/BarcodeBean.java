package org.icgc.dcc.pcawg.client.data.barcode;

import com.opencsv.bean.CsvBindByName;
import lombok.Data;
import lombok.NonNull;
import lombok.val;

@Data
public class BarcodeBean {

  public static BarcodeBean newBarcodeBean(String project, String entity_type, String uuid, String barcode){
    val b = new BarcodeBean();
    b.setUuid(uuid);
    b.setBarcode(barcode);
    b.setEntity_type(entity_type);
    b.setProject(project);
    return b;
  }

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
