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
public class SampleSheetSearchRequest implements SearchRequest<SampleSheetSearchRequest>, Serializable {
  public static final long serialVersionUID = 1490934538L;

  @CsvBindByName(required=true) @NonNull private String  donor_unique_id;
  @CsvBindByName(required=true) @NonNull private String  library_strategy;
  @CsvBindByName(required=true) @NonNull private String  dcc_specimen_type;
  @CsvBindByName(required=true) @NonNull private String  aliquot_id;

  @Override
  public boolean matches(SampleSheetSearchRequest request) {
    boolean result = matchFunctions(request, SampleSheetSearchRequest::getAliquot_id, String::equals);
    result &= matchFunctions(request, SampleSheetSearchRequest::getLibrary_strategy, String::equals);
    result &= matchFunctions(request, SampleSheetSearchRequest::getDcc_specimen_type, SearchRequest::lowercaseAndContains);
    result &= matchFunctions(request, SampleSheetSearchRequest::getDonor_unique_id, String::equals);
    return result;
  }

  private boolean matchFunctions(SampleSheetSearchRequest rr, Function<SampleSheetSearchRequest, String> functor, BiPredicate<String, String> comparingFunctor){
    Object s = new Object();
    s.equals("");
    return SearchRequest.matchFunctions(this, rr,functor, comparingFunctor );
  }

}
