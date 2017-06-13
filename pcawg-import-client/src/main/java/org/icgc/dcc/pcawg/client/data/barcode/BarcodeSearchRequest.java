package org.icgc.dcc.pcawg.client.data.barcode;

import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.data.SearchRequest;

import java.io.Serializable;

@Value
public class BarcodeSearchRequest implements SearchRequest<BarcodeSearchRequest> , Serializable {

  public static final long serialVersionUID = 1490934539L;

  public static BarcodeSearchRequest newBarcodeRequest(String uuid){
    return new BarcodeSearchRequest(uuid);
  }

  @NonNull private final String uuid;

  @Override
  public boolean matches(BarcodeSearchRequest request) {
    return SearchRequest.matchFunctions(this, request,
        BarcodeSearchRequest::getUuid, String::equals); }



}
