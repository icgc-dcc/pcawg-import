package org.icgc.dcc.pcawg.client.data.barcode;

import java.util.List;

public interface BarcodeDao<B, R> {

  List<B> find(R request);

  List<B> findAll();

}
