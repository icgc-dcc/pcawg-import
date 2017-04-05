package org.icgc.dcc.pcawg.client.data.barcode;

import java.util.List;

public interface BarcodeSheetDao<B, R> {

  List<B> find(R request);

  List<B> findAll();

}
