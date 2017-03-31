package org.icgc.dcc.pcawg.client.data.ega;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.data.sample.SearchRequest;

import java.util.function.BiPredicate;
import java.util.function.Function;

@Value
@Builder
public class EgaSearchRequest implements SearchRequest<EgaSearchRequest> {

  public static EgaSearchRequest fromEgaBean(EgaBean b) {
    return new EgaSearchRequest(b.getIcgc_project_code(), b.getSubmitter_sample_id());
  }

  @NonNull private final String dccProjectCode;
  @NonNull private final String submitterSampleId;

  @Override
  public boolean matches(EgaSearchRequest request) {
    boolean result = matchFunctions(request, EgaSearchRequest::getDccProjectCode, String::equals);
    result &= matchFunctions(request, EgaSearchRequest::getSubmitterSampleId, String::equals);
    return result;
  }

  private boolean matchFunctions(EgaSearchRequest rr, Function<EgaSearchRequest, String> functor,
      BiPredicate<String, String> comparingFunctor){
    return SearchRequest.matchFunctions(this, rr,functor, comparingFunctor );
  }
}
