package org.icgc.dcc.pcawg.client.data.barcode;

import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.data.sample.SearchRequest;

@Value
public class BarcodeSearchRequest implements SearchRequest<BarcodeSearchRequest> {

  public static BarcodeSearchRequest newBarcodeRequest(String uuid){
    return new BarcodeSearchRequest(uuid);
  }

  @NonNull private final String uuid;

  @Override
  public boolean matches(BarcodeSearchRequest request) {
    return SearchRequest.matchFunctions(this, request,
        BarcodeSearchRequest::getUuid, String::equals); }



}
