package org.icgc.dcc.pcawg.client.data.sample.impl;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.AbstractFileDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest.SampleSearchRequestBuilder;
import org.icgc.dcc.pcawg.client.model.beans.SampleBean;
import org.icgc.dcc.pcawg.client.utils.ObjectPersistance;

import java.io.Reader;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
public class SampleBeanDao extends AbstractFileDao<SampleBean, SampleSearchRequest> implements Serializable,
    SampleDao<SampleBean, SampleSearchRequest> {

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
    makeUuidsLowercase(getBeans());
  }

  private SampleBeanDao(Reader reader) {
    super(reader);
    makeUuidsLowercase(getBeans());
  }

  private SampleBeanDao(List<SampleBean> beans){
    super(beans);
    makeUuidsLowercase(getBeans());
  }

  private static boolean isWildcardSearch(String value){
    val isWild = EMPTY.equals(value) || STAR.equals(value);
    return value == null || isWild;
  }

  private static void makeUuidsLowercase(List<SampleBean> beans){
    log.info("Making uuids for SampleBeans lowercase ...");
    beans.stream()
        .filter(SampleBean::isUsProject)
        .map(s ->     transform(s, SampleBean::getSubmitter_donor_id,    String::toLowerCase ,  s::setSubmitter_donor_id    ))
        .map(s ->     transform(s, SampleBean::getSubmitter_sample_id,   String::toLowerCase ,  s::setSubmitter_sample_id   ))
        .map(s ->     transform(s, SampleBean::getSubmitter_specimen_id, String::toLowerCase ,  s::setSubmitter_specimen_id ))
        .forEach(s -> transform(s, SampleBean::getAliquot_id,            String::toLowerCase ,  s::setAliquot_id            ));
  }

  private static SampleBean transform(SampleBean bean, Function<SampleBean, String> getter,
      Function<String, String> stringFunctor,
      Consumer<String> setter){
    val oldValue = getter.apply(bean);
    val newValue = stringFunctor.apply(oldValue);
    setter.accept(newValue);
    return bean;
  }

  private static boolean decideAndCompare(SampleSearchRequest request,
      SampleBean sampleBean, Function<SampleSearchRequest, String> requestFunctor,
      Function<SampleBean, String> actualFunctor,  BiPredicate<String, String> comparingBiPredicate) {
    val searchValue = requestFunctor.apply(request);
    val actualValue = actualFunctor.apply(sampleBean);
    if (isWildcardSearch(searchValue)){
      return true;
    } else {
      return comparingBiPredicate.test(actualValue, searchValue);
    }
  }

  private static Predicate<SampleBean> createEqualsPredicate(SampleSearchRequest request, Function<SampleSearchRequest,
      String> requestFunctor, Function<SampleBean, String> actualFunctor ){
    return s -> decideAndCompare(request, s, requestFunctor, actualFunctor, String::equals);
  }

  private static Predicate<SampleBean> createContainsLowercasePredicate(SampleSearchRequest request, Function<SampleSearchRequest,
      String> requestFunctor, Function<SampleBean, String> actualFunctor ){
    return s -> decideAndCompare(request, s, requestFunctor, actualFunctor, (actual,req) -> actual.toLowerCase().contains(req.toLowerCase()));
  }

  public static SampleBeanDao newSampleBeanDao(String inputFilename){
    return new SampleBeanDao(inputFilename);
  }

  public static SampleBeanDao newSampleBeanDao(Reader reader){
    return new SampleBeanDao(reader);
  }

  public static SampleBeanDao newSampleBeanDao(List<SampleBean> beans){
    return new SampleBeanDao(beans);
  }

  @SneakyThrows
  public static SampleBeanDao restoreSampleBeanDao(String storedSampleDaoFilename){
    return (SampleBeanDao) ObjectPersistance.restore(storedSampleDaoFilename);
  }

  @Override public Optional<SampleBean> findFirstAliquotId(String aliquotId){
    val request  = createWildcardRequestBuilder()
                    .aliquot_id(aliquotId)
                    .build();
    return find(request).stream().findFirst();
  }

  @Override public List<SampleBean> findAliquotId(String aliquotId){
    val request  = createWildcardRequestBuilder()
        .aliquot_id(aliquotId)
        .build();
    return find(request);
  }

  @Override public Optional<SampleBean> findFirstDonorUniqueId(String donorUniqueId){
    val request  = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .build();
    return find(request).stream().findFirst();
  }

  @Override public List<SampleBean> findDonorUniqueId(String donorUniqueId){
    val request  = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .build();
    return find(request);
  }

  @Override public List<SampleBean> findAll() {
    return ImmutableList.copyOf(getBeans());
  }

  @Override
  protected SampleSearchRequest createRequestFromBean(SampleBean bean) {
    return SampleSearchRequest.builder()
        .library_strategy(bean.getLibrary_strategy())
        .dcc_specimen_type(bean.getDcc_specimen_type())
        .aliquot_id(bean.getAliquot_id())
        .donor_unique_id(bean.getDonor_unique_id())
        .build();
  }

}
