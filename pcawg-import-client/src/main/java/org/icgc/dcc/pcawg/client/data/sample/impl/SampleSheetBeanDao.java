package org.icgc.dcc.pcawg.client.data.sample.impl;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.AbstractFileDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetSearchRequest;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetSearchRequest.SampleSheetSearchRequestBuilder;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetBean;
import org.icgc.dcc.pcawg.client.utils.persistance.ObjectPersistance;

import java.io.Reader;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

@Slf4j
public class SampleSheetBeanDao extends AbstractFileDao<SampleSheetBean, SampleSheetSearchRequest> implements Serializable,
    SampleSheetDao<SampleSheetBean, SampleSheetSearchRequest> {

  private static final long serialVersionUID = 1490628519L;
  private static final String STAR = "*";
  private static final String EMPTY = "";

  @Override
  public Class<SampleSheetBean> getBeanClass() {
    return SampleSheetBean.class;
  }

  public static final SampleSheetSearchRequestBuilder createWildcardRequestBuilder(){
    return SampleSheetSearchRequest.builder()
        .aliquot_id(STAR)
        .dcc_specimen_type(STAR)
        .donor_unique_id(STAR)
        .library_strategy(STAR);
  }

  private SampleSheetBeanDao(String inputFilename) {
    super(inputFilename);
    makeUuidsLowercase(getBeans());
  }

  private SampleSheetBeanDao(Reader reader) {
    super(reader);
    makeUuidsLowercase(getBeans());
  }

  private SampleSheetBeanDao(List<SampleSheetBean> beans){
    super(beans);
    makeUuidsLowercase(getBeans());
  }

  private static boolean isWildcardSearch(String value){
    val isWild = EMPTY.equals(value) || STAR.equals(value);
    return value == null || isWild;
  }

  private static void makeUuidsLowercase(List<SampleSheetBean> beans){
    log.info("Making uuids for SampleBeans lowercase ...");
    beans.stream()
        .filter(SampleSheetBean::isUsProject)
        .map(s ->     transform(s, SampleSheetBean::getSubmitter_donor_id,    String::toLowerCase ,  s::setSubmitter_donor_id    ))
        .map(s ->     transform(s, SampleSheetBean::getSubmitter_sample_id,   String::toLowerCase ,  s::setSubmitter_sample_id   ))
        .map(s ->     transform(s, SampleSheetBean::getSubmitter_specimen_id, String::toLowerCase ,  s::setSubmitter_specimen_id ))
        .forEach(s -> transform(s, SampleSheetBean::getAliquot_id,            String::toLowerCase ,  s::setAliquot_id            ));
  }

  private static SampleSheetBean transform(SampleSheetBean bean, Function<SampleSheetBean, String> getter,
      Function<String, String> stringFunctor,
      Consumer<String> setter){
    val oldValue = getter.apply(bean);
    val newValue = stringFunctor.apply(oldValue);
    setter.accept(newValue);
    return bean;
  }

  private static boolean decideAndCompare(SampleSheetSearchRequest request,
      SampleSheetBean sampleSheetBean, Function<SampleSheetSearchRequest, String> requestFunctor,
      Function<SampleSheetBean, String> actualFunctor,  BiPredicate<String, String> comparingBiPredicate) {
    val searchValue = requestFunctor.apply(request);
    val actualValue = actualFunctor.apply(sampleSheetBean);
    if (isWildcardSearch(searchValue)){
      return true;
    } else {
      return comparingBiPredicate.test(actualValue, searchValue);
    }
  }

  private static Predicate<SampleSheetBean> createEqualsPredicate(SampleSheetSearchRequest request, Function<SampleSheetSearchRequest,
      String> requestFunctor, Function<SampleSheetBean, String> actualFunctor ){
    return s -> decideAndCompare(request, s, requestFunctor, actualFunctor, String::equals);
  }

  private static Predicate<SampleSheetBean> createContainsLowercasePredicate(SampleSheetSearchRequest request, Function<SampleSheetSearchRequest,
      String> requestFunctor, Function<SampleSheetBean, String> actualFunctor ){
    return s -> decideAndCompare(request, s, requestFunctor, actualFunctor, (actual,req) -> actual.toLowerCase().contains(req.toLowerCase()));
  }

  public static SampleSheetBeanDao newSampleSheetBeanDao(String inputFilename){
    return new SampleSheetBeanDao(inputFilename);
  }

  public static SampleSheetBeanDao newSampleSheetBeanDao(Reader reader){
    return new SampleSheetBeanDao(reader);
  }

  public static SampleSheetBeanDao newSampleSheetBeanDao(List<SampleSheetBean> beans){
    return new SampleSheetBeanDao(beans);
  }

  @SneakyThrows
  public static SampleSheetBeanDao restoreSampleBeanDao(String storedSampleDaoFilename){
    return (SampleSheetBeanDao) ObjectPersistance.restore(storedSampleDaoFilename);
  }

  @Override public Optional<SampleSheetBean> findFirstAliquotId(String aliquotId){
    val request  = createWildcardRequestBuilder()
                    .aliquot_id(aliquotId)
                    .build();
    return find(request).stream().findFirst();
  }

  @Override public List<SampleSheetBean> findAliquotId(String aliquotId){
    val request  = createWildcardRequestBuilder()
        .aliquot_id(aliquotId)
        .build();
    return find(request);
  }

  @Override public Optional<SampleSheetBean> findFirstDonorUniqueId(String donorUniqueId){
    val request  = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .build();
    return find(request).stream().findFirst();
  }

  @Override public List<SampleSheetBean> findDonorUniqueId(String donorUniqueId){
    val request  = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .build();
    return find(request);
  }

  @Override public List<SampleSheetBean> findAll() {
    return ImmutableList.copyOf(getBeans());
  }

  @Override
  protected SampleSheetSearchRequest createRequestFromBean(SampleSheetBean bean) {
    return SampleSheetSearchRequest.builder()
        .library_strategy(bean.getLibrary_strategy())
        .dcc_specimen_type(bean.getDcc_specimen_type())
        .aliquot_id(bean.getAliquot_id())
        .donor_unique_id(bean.getDonor_unique_id())
        .build();
  }

}
