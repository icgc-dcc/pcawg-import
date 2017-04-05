package org.icgc.dcc.pcawg.client.data.sample;

import com.opencsv.bean.CsvBindByName;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import org.icgc.dcc.pcawg.client.data.SearchRequest;

import java.io.Serializable;
import java.util.function.BiPredicate;
import java.util.function.Function;

@Data
@Builder
public class SampleSearchRequest implements SearchRequest<SampleSearchRequest>, Serializable {
  public static final long serialVersionUID = 1490934538L;

  @CsvBindByName(required=true) @NonNull private String  donor_unique_id;
  @CsvBindByName(required=true) @NonNull private String  library_strategy;
  @CsvBindByName(required=true) @NonNull private String  dcc_specimen_type;
  @CsvBindByName(required=true) @NonNull private String  aliquot_id;

  @Override
  public boolean matches(SampleSearchRequest request) {
    boolean result = matchFunctions(request, SampleSearchRequest::getAliquot_id, String::equals);
    result &= matchFunctions(request, SampleSearchRequest::getLibrary_strategy, String::equals);
    result &= matchFunctions(request, SampleSearchRequest::getDcc_specimen_type, SearchRequest::lowercaseAndContains);
    result &= matchFunctions(request, SampleSearchRequest::getDonor_unique_id, String::equals);
    return result;
  }

  private boolean matchFunctions(SampleSearchRequest rr, Function<SampleSearchRequest, String> functor, BiPredicate<String, String> comparingFunctor){
    Object s = new Object();
    s.equals("");
    return SearchRequest.matchFunctions(this, rr,functor, comparingFunctor );
  }

}
