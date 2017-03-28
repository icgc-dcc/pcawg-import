package org.icgc.dcc.pcawg.client.data.sample;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.AbstractFileDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest.SampleSearchRequestBuilder;
import org.icgc.dcc.pcawg.client.utils.ObjectPersistance;

import java.io.Reader;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

@Slf4j
public class SampleBeanDao extends AbstractFileDao<SampleBean, SampleSearchRequest> implements Serializable, SampleDao<SampleBean, SampleSearchRequest> {

  private static final long serialVersionUID = 1490628519L;
  private static final String STAR = "*";
  private static final String EMPTY = "";

  @Override
  public Class<SampleBean> getBeanClass() {
    return SampleBean.class;
  }

  public static final SampleSearchRequestBuilder createWildcardRequestBuilder(){
    return SampleSearchRequest.builder()
        .aliquot_id(STAR)
        .dcc_specimen_type(STAR)
        .donor_unique_id(STAR)
        .library_strategy(STAR);
  }

  private SampleBeanDao(String inputFilename) {
    super(inputFilename);
  }

  private SampleBeanDao(Reader reader) {
    super(reader);
  }

  private static boolean isWildcardSearch(String value){
    val isWild = EMPTY.equals(value) || STAR.equals(value);
    return value == null || isWild;
  }

  private static boolean decide(SampleSearchRequest request, SampleBean sampleBean, Function<SampleSearchRequest, String> requestFunctor, Function<SampleBean, String> actualFunctor) {
    val searchValue = requestFunctor.apply(request);
    val actualValue = actualFunctor.apply(sampleBean);
    if (isWildcardSearch(searchValue)){
      return true;
    } else {
      return actualValue.equals(searchValue);
    }
  }

  private static Predicate<SampleBean> createPredicate(SampleSearchRequest request, Function<SampleSearchRequest, String> requestFunctor, Function<SampleBean, String> actualFunctor ){
    return s -> decide(request, s, requestFunctor, actualFunctor);
  }

  public static SampleDao<SampleBean, SampleSearchRequest> newSampleBeanDao(String inputFilename){
    return new SampleBeanDao(inputFilename);
  }

  public static SampleDao<SampleBean, SampleSearchRequest> newSampleBeanDao(Reader reader){
    return new SampleBeanDao(reader);
  }

  @SneakyThrows
  public static SampleBeanDaoOld restoreSampleBeanDao(String storedSampleDaoFilename){
    return (SampleBeanDaoOld) ObjectPersistance.restore(storedSampleDaoFilename);
  }

  @SneakyThrows
  private Stream<SampleBean>  streamAll(SampleSearchRequest request){
    return getData().stream()
        .filter(createPredicate(request, SampleSearchRequest::getAliquot_id, SampleBean::getAliquot_id))
        .filter(createPredicate(request, SampleSearchRequest::getDcc_specimen_type, SampleBean::getDcc_specimen_type))
        .filter(createPredicate(request, SampleSearchRequest::getDonor_unique_id, SampleBean::getDonor_unique_id))
        .filter(createPredicate(request, SampleSearchRequest::getLibrary_strategy, SampleBean::getLibrary_strategy));
  }

  @Override
  public List<SampleBean> find(SampleSearchRequest request){
    return streamAll(request)
        .collect(toImmutableList());
  }

  @Override public Optional<SampleBean> findFirstAliquotId(String aliquotId){
    val request  = createWildcardRequestBuilder()
                    .aliquot_id(aliquotId)
                    .build();
    return streamAll(request).findFirst();
  }

  @Override public List<SampleBean> findAliquotId(String aliquotId){
    val request  = createWildcardRequestBuilder()
        .aliquot_id(aliquotId)
        .build();
    return streamAll(request).collect(toImmutableList());
  }

  @Override public Optional<SampleBean> findFirstDonorUniqueId(String donorUniqueId){
    val request  = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .build();
    return streamAll(request).findFirst();
  }

  @Override public List<SampleBean> findDonorUniqueId(String donorUniqueId){
    val request  = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .build();
    return streamAll(request).collect(toImmutableList());
  }

}
