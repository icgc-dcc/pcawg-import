package org.icgc.dcc.pcawg.client.data.sample.impl;

import com.google.common.collect.ImmutableList;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetSearchRequest;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetSearchRequest.SampleSheetSearchRequestBuilder;
import org.icgc.dcc.pcawg.client.utils.persistance.ObjectPersistance;

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
public class SampleBeanSheetDaoOld implements Serializable, SampleSheetDao<SampleSheetBean, SampleSheetSearchRequest> {

  private static final long serialVersionUID = 1490628519L;
  private static final char SEPERATOR = '\t';
  private static final String STAR = "*";
  private static final String EMPTY = "";


  public static final SampleSheetSearchRequestBuilder createWildcardRequestBuilder(){
    return SampleSheetSearchRequest.builder()
        .aliquot_id(STAR)
        .dcc_specimen_type(STAR)
        .donor_unique_id(STAR)
        .library_strategy(STAR);
  }


  private static List<SampleSheetBean> convert(Reader reader){
    val csvReader = new CSVReader(reader, SEPERATOR);
    val strategy = new HeaderColumnNameMappingStrategy<SampleSheetBean>();
    strategy.setType(SampleSheetBean.class);
    val csvToBean = new CsvToBean<SampleSheetBean>();
    return csvToBean.parse(strategy, csvReader);
  }

  private static boolean isWildcardSearch(String value){
    val isWild = EMPTY.equals(value) || STAR.equals(value);
    return value == null || isWild;
  }

  private static boolean decide(SampleSheetSearchRequest request, SampleSheetBean sampleSheetBean, Function<SampleSheetSearchRequest, String> requestFunctor, Function<SampleSheetBean, String> actualFunctor) {
    val searchValue = requestFunctor.apply(request);
    val actualValue = actualFunctor.apply(sampleSheetBean);
    if (isWildcardSearch(searchValue)){
      return true;
    } else {
      return actualValue.equals(searchValue);
    }
  }

  private static Predicate<SampleSheetBean> createPredicate(SampleSheetSearchRequest request, Function<SampleSheetSearchRequest, String> requestFunctor, Function<SampleSheetBean, String> actualFunctor ){
    return s -> decide(request, s, requestFunctor, actualFunctor);
  }

  public static SampleBeanSheetDaoOld newSampleBeanDaoOld(String inputFilename){
    return new SampleBeanSheetDaoOld(inputFilename);
  }

  public static SampleBeanSheetDaoOld newSampleBeanDaoOld(Reader reader){
    return new SampleBeanSheetDaoOld(reader);
  }

  @SneakyThrows
  public static SampleBeanSheetDaoOld restoreSampleBeanDao(String storedSampleDaoFilename){
    return (SampleBeanSheetDaoOld) ObjectPersistance.restore(storedSampleDaoFilename);
  }

  private final Reader reader;

  private List<SampleSheetBean> beans;

  @SneakyThrows
  private SampleBeanSheetDaoOld(String inputFilename) {
    val file = Paths.get(inputFilename).toFile();
    checkState(file.exists(),"The inputFilename [%s] does not exist", file.getAbsolutePath());
    checkState(file.isFile(),"The inputFilename [%s] is not a file" , file.getAbsolutePath());
    this.reader = new FileReader(file);
    this.beans = convert(reader);
    this.reader.close();
    log.info("Done Coverting inputFilename: {} to SampleBeanDaoOld", inputFilename);
  }

  private SampleBeanSheetDaoOld(Reader reader) {
    this.reader = reader;
    this.beans = convert(reader);
    log.info("Done Coverting Reader to SampleBeanDaoOld");
  }

  @SneakyThrows
  private Stream<SampleSheetBean>  streamAll(SampleSheetSearchRequest request){
    return beans.stream()
        .filter(createPredicate(request, SampleSheetSearchRequest::getAliquot_id, SampleSheetBean::getAliquot_id))
        .filter(createPredicate(request, SampleSheetSearchRequest::getDcc_specimen_type, SampleSheetBean::getDcc_specimen_type))
        .filter(createPredicate(request, SampleSheetSearchRequest::getDonor_unique_id, SampleSheetBean::getDonor_unique_id))
        .filter(createPredicate(request, SampleSheetSearchRequest::getLibrary_strategy, SampleSheetBean::getLibrary_strategy));
  }

  public void store(String filename) throws IOException{
    ObjectPersistance.store(this, filename);
  }


  public List<SampleSheetBean> find(SampleSheetSearchRequest request){
    return streamAll(request)
        .collect(toImmutableList());
  }

  public Optional<SampleSheetBean> findFirstAliquotId(String aliquotId){
    val request  = createWildcardRequestBuilder()
                    .aliquot_id(aliquotId)
                    .build();
    return streamAll(request).findFirst();
  }

  public List<SampleSheetBean> findAliquotId(String aliquotId){
    val request  = createWildcardRequestBuilder()
        .aliquot_id(aliquotId)
        .build();
    return streamAll(request).collect(toImmutableList());
  }

  public Optional<SampleSheetBean> findFirstDonorUniqueId(String donorUniqueId){
    val request  = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .build();
    return streamAll(request).findFirst();
  }

  public List<SampleSheetBean> findDonorUniqueId(String donorUniqueId){
    val request  = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .build();
    return streamAll(request).collect(toImmutableList());
  }

  @Override public List<SampleSheetBean> findAll() {
    return ImmutableList.copyOf(beans);
  }
}
