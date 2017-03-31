package org.icgc.dcc.pcawg.client.data.exp;

import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.data.sample.SearchRequest;

import static org.icgc.dcc.pcawg.client.data.exp.UuidSearchField.newUuidSearchField;

@Value
public class BarcodeSearchRequest2 implements SearchRequest<BarcodeSearchRequest2> {

  public static BarcodeSearchRequest2 newBarcodeRequest(String uuid){
    return new BarcodeSearchRequest2(newUuidSearchField(uuid));
  }

  @NonNull private final UuidSearchField uuidField;

  @Override
  public boolean matches(BarcodeSearchRequest2 request) {
    return SearchRequest.matchFunctions(this, request,
        x -> x.getUuidField().get(), String::equals); }



}
