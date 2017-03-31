package org.icgc.dcc.pcawg.client.data.exp;

import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.icgc.dcc.pcawg.client.data.sample.SearchRequest;

import java.util.function.BiPredicate;
import java.util.function.Function;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.pcawg.client.data.exp.StringSearchField.newStringSearchField;
import static org.icgc.dcc.pcawg.client.data.exp.StringSearchField.newWildStringSearchField;
import static org.icgc.dcc.pcawg.client.data.exp.UuidSearchField.newUuidSearchField;
import static org.icgc.dcc.pcawg.client.data.exp.UuidSearchField.newWildUuidSearchField;

@Value
@RequiredArgsConstructor(access = PRIVATE)
@Builder
public class SampleSearchRequest2 implements SearchRequest<SampleSearchRequest2> {

  public static SampleSearchRequest2 newSampleSearchRequest(
                    String donor_unique_id,
                    String library_strategy,
                    String dcc_specimen_type,
                    String aliquot_id){
    return SampleSearchRequest2.builder()
        .donor_unique_id(newStringSearchField(donor_unique_id))
        .library_strategy(newStringSearchField(library_strategy))
        .dcc_specimen_type(newStringSearchField(dcc_specimen_type))
        .aliquot_id(newUuidSearchField(aliquot_id))
        .build();
  }


  public static SampleSearchRequest2Builder wildcardBuilder(){
        return SampleSearchRequest2.builder()
            .aliquot_id(newWildUuidSearchField())
            .dcc_specimen_type(newWildStringSearchField())
            .donor_unique_id(newWildStringSearchField())
            .library_strategy(newWildStringSearchField());
  }

  @NonNull private StringSearchField  donor_unique_id;
  @NonNull private StringSearchField  library_strategy;
  @NonNull private StringSearchField  dcc_specimen_type;
  @NonNull private UuidSearchField    aliquot_id;


  @Override
  public boolean matches(SampleSearchRequest2 request) {
    boolean result = matchFunctions(request, x -> x.getAliquot_id().get(), String::equals);
    result &= matchFunctions(request, x -> x.getLibrary_strategy().get(), String::equals);
    result &= matchFunctions(request, x -> x.getDcc_specimen_type().get(), String::equals);
    result &= matchFunctions(request, x -> x.getDonor_unique_id().get(), String::equals);
    return result;
  }

  private boolean matchFunctions(SampleSearchRequest2 rr, Function<SampleSearchRequest2, String> functor, BiPredicate<String, String> comparingFunctor){
    return SearchRequest.matchFunctions(this, rr,functor, comparingFunctor );
  }

}
