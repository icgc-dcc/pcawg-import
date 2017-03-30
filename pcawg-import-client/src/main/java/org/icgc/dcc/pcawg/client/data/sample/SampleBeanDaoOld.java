package org.icgc.dcc.pcawg.client.data.sample;

import com.google.common.collect.ImmutableList;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest.SampleSearchRequestBuilder;
import org.icgc.dcc.pcawg.client.utils.ObjectPersistance;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

@Slf4j
public class SampleBeanDaoOld implements Serializable, SampleDao<SampleBean, SampleSearchRequest> {

  private static final long serialVersionUID = 1490628519L;
  private static final char SEPERATOR = '\t';
  private static final String STAR = "*";
  private static final String EMPTY = "";


  public static final SampleSearchRequestBuilder createWildcardRequestBuilder(){
    return SampleSearchRequest.builder()
        .aliquot_id(STAR)
        .dcc_specimen_type(STAR)
        .donor_unique_id(STAR)
        .library_strategy(STAR);
  }


  private static List<SampleBean> convert(Reader reader){
    val csvReader = new CSVReader(reader, SEPERATOR);
    val strategy = new HeaderColumnNameMappingStrategy<SampleBean>();
    strategy.setType(SampleBean.class);
    val csvToBean = new CsvToBean<SampleBean>();
    return csvToBean.parse(strategy, csvReader);
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

  public static SampleBeanDaoOld newSampleBeanDaoOld(String inputFilename){
    return new SampleBeanDaoOld(inputFilename);
  }

  public static SampleBeanDaoOld newSampleBeanDaoOld(Reader reader){
    return new SampleBeanDaoOld(reader);
  }

  @SneakyThrows
  public static SampleBeanDaoOld restoreSampleBeanDao(String storedSampleDaoFilename){
    return (SampleBeanDaoOld) ObjectPersistance.restore(storedSampleDaoFilename);
  }

  private final Reader reader;

  private List<SampleBean> beans;

  @SneakyThrows
  private SampleBeanDaoOld(String inputFilename) {
    val file = Paths.get(inputFilename).toFile();
    checkState(file.exists(),"The inputFilename [%s] does not exist", file.getAbsolutePath());
    checkState(file.isFile(),"The inputFilename [%s] is not a file" , file.getAbsolutePath());
    this.reader = new FileReader(file);
    this.beans = convert(reader);
    this.reader.close();
    log.info("Done Coverting inputFilename: {} to SampleBeanDaoOld", inputFilename);
  }

  private SampleBeanDaoOld(Reader reader) {
    this.reader = reader;
    this.beans = convert(reader);
    log.info("Done Coverting Reader to SampleBeanDaoOld");
  }

  @SneakyThrows
  private Stream<SampleBean>  streamAll(SampleSearchRequest request){
    return beans.stream()
        .filter(createPredicate(request, SampleSearchRequest::getAliquot_id, SampleBean::getAliquot_id))
        .filter(createPredicate(request, SampleSearchRequest::getDcc_specimen_type, SampleBean::getDcc_specimen_type))
        .filter(createPredicate(request, SampleSearchRequest::getDonor_unique_id, SampleBean::getDonor_unique_id))
        .filter(createPredicate(request, SampleSearchRequest::getLibrary_strategy, SampleBean::getLibrary_strategy));
  }

  public void store(String filename) throws IOException{
    ObjectPersistance.store(this, filename);
  }


  public List<SampleBean> find(SampleSearchRequest request){
    return streamAll(request)
        .collect(toImmutableList());
  }

  public Optional<SampleBean> findFirstAliquotId(String aliquotId){
    val request  = createWildcardRequestBuilder()
                    .aliquot_id(aliquotId)
                    .build();
    return streamAll(request).findFirst();
  }

  public List<SampleBean> findAliquotId(String aliquotId){
    val request  = createWildcardRequestBuilder()
        .aliquot_id(aliquotId)
        .build();
    return streamAll(request).collect(toImmutableList());
  }

  public Optional<SampleBean> findFirstDonorUniqueId(String donorUniqueId){
    val request  = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .build();
    return streamAll(request).findFirst();
  }

  public List<SampleBean> findDonorUniqueId(String donorUniqueId){
    val request  = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .build();
    return streamAll(request).collect(toImmutableList());
  }

  @Override public List<SampleBean> findAll() {
    return ImmutableList.copyOf(beans);
  }
}
