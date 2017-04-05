package org.icgc.dcc.pcawg.client.data.barcode.impl;

import lombok.NoArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.model.beans.BarcodeBean;

import static com.google.common.base.Preconditions.checkArgument;
import static lombok.AccessLevel.PRIVATE;

@NoArgsConstructor(access = PRIVATE)
public class BarcodeSheetParser {

  private static final int PROJECT_POS = 0;
  private static final int ENTITY_TYPE_POS = 1;
  private static final int UUID_POS = 2;
  private static final int BARCODE_POS = 3;
  private static final int MAX_NUM_COLUMNS = 4;
  private static final String TAB = "\t";


  public static BarcodeBean parseLine(String tsvLine){
    val a = tsvLine.trim().split(TAB);
    checkArgument(a.length == MAX_NUM_COLUMNS, "Max allowed columns is %s, but input columns is %s", MAX_NUM_COLUMNS, a.length);
    val b = new BarcodeBean();
    b.setProject(a[PROJECT_POS]);
    b.setEntity_type(a[ENTITY_TYPE_POS]);
    b.setUuid(a[UUID_POS]);
    b.setBarcode(a[BARCODE_POS]);
    return b;
  }


}
