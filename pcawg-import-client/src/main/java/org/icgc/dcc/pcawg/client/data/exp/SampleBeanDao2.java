package org.icgc.dcc.pcawg.client.data.exp;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.AbstractFileDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest;
import org.icgc.dcc.pcawg.client.utils.ObjectPersistance;

import java.io.Reader;
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.icgc.dcc.pcawg.client.data.exp.StringSearchField.newStringSearchField;
import static org.icgc.dcc.pcawg.client.data.exp.UuidSearchField.newUuidSearchField;
import static org.icgc.dcc.pcawg.client.data.exp.SampleSearchRequest2.newSampleSearchRequest;

@Slf4j
public class SampleBeanDao2 extends AbstractFileDao<SampleBean, SampleSearchRequest2> implements Serializable,
    SampleDao<SampleBean, SampleSearchRequest2> {

  private static final long serialVersionUID = 1490628519L;
  private static final String STAR = "*";
  private static final String EMPTY = "";

  @Override
  public Class<SampleBean> getBeanClass() {
    return SampleBean.class;
  }


  private SampleBeanDao2(String inputFilename) {
    super(inputFilename);
//    denormalizeBeans(getBeans());
  }

  private SampleBeanDao2(Reader reader) {
    super(reader);
//    denormalizeBeans(getBeans());
  }

  private SampleBeanDao2(List<SampleBean> beans){
    super(beans);
//    denormalizeBeans(getBeans());
  }

  private static boolean isWildcardSearch(String value){
    val isWild = EMPTY.equals(value) || STAR.equals(value);
    return value == null || isWild;
  }

  private static void denormalizeBeans(List<SampleBean> beans){
    log.info("Denormalizing SampleBeans with lowercase uuids");
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

  public static SampleBeanDao2 newSampleBeanDao(String inputFilename){
    return new SampleBeanDao2(inputFilename);
  }

  public static SampleBeanDao2 newSampleBeanDao(Reader reader){
    return new SampleBeanDao2(reader);
  }

  public static SampleBeanDao2 newSampleBeanDao(List<SampleBean> beans){
    return new SampleBeanDao2(beans);
  }

  @SneakyThrows
  public static SampleBeanDao2 restoreSampleBeanDao(String storedSampleDaoFilename){
    return (SampleBeanDao2) ObjectPersistance.restore(storedSampleDaoFilename);
  }

  @Override public Optional<SampleBean> findFirstAliquotId(String aliquotId){
    return findAliquotId(aliquotId).stream().findFirst();
  }

  @Override public List<SampleBean> findAliquotId(String aliquotId){
    val request  = SampleSearchRequest2.wildcardBuilder()
        .aliquot_id(newUuidSearchField(aliquotId))
        .build();
    return find(request);
  }

  @Override public Optional<SampleBean> findFirstDonorUniqueId(String donorUniqueId){
    return findDonorUniqueId(donorUniqueId).stream().findFirst();
  }

  @Override public List<SampleBean> findDonorUniqueId(String donorUniqueId){
    val request  = SampleSearchRequest2.wildcardBuilder()
        .donor_unique_id(newStringSearchField(donorUniqueId))
        .build();
    return find(request);
  }

  @Override public List<SampleBean> findAll() {
    return ImmutableList.copyOf(getBeans());
  }

  @Override
  protected SampleSearchRequest2 createRequestFromBean(SampleBean bean) {
    return newSampleSearchRequest(bean.getDonor_unique_id(), bean.getLibrary_strategy(),
        bean.getDcc_specimen_type(), bean.getAliquot_id());
  }

}
