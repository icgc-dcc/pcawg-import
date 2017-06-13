package org.icgc.dcc.pcawg.client.data.ega;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.data.SearchRequest;

import java.util.function.BiPredicate;
import java.util.function.Function;

@Value
@Builder
public class EgaSheetSearchRequest implements SearchRequest<EgaSheetSearchRequest> {

  public static EgaSheetSearchRequest fromEgaBean(EgaSheetBean b) {
    return new EgaSheetSearchRequest(b.getIcgc_project_code(), b.getSubmitter_sample_id());
  }

  @NonNull private final String dccProjectCode;
  @NonNull private final String submitterSampleId;

  @Override
  public boolean matches(EgaSheetSearchRequest request) {
    boolean result = matchFunctions(request, EgaSheetSearchRequest::getDccProjectCode, String::equals);
    result &= matchFunctions(request, EgaSheetSearchRequest::getSubmitterSampleId, String::equals);
    return result;
  }

  private boolean matchFunctions(EgaSheetSearchRequest rr, Function<EgaSheetSearchRequest, String> functor,
      BiPredicate<String, String> comparingFunctor){
    return SearchRequest.matchFunctions(this, rr,functor, comparingFunctor );
  }
}
