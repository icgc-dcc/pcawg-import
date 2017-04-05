package org.icgc.dcc.pcawg.client.data;

import com.google.common.collect.ImmutableList;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.utils.ObjectPersistance;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.groupingBy;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

//TODO: too many concerns here. Need to separate this into AbstractBeanDao, which will have the find method
@Slf4j
public abstract class AbstractFileDao<B, R extends SearchRequest<R>> implements Serializable {

  public static final long serialVersionUID = 1490930264L;
  private static final char SEPERATOR = '\t';

  private transient Reader reader;

  @Getter(AccessLevel.PROTECTED)
  private final List<B> beans;
  private final Map<R,List<B>> requestMap;

  @SneakyThrows
  protected List<B> convert(){
    log.info("Converting input Reader to collection of beans of class: {}", getBeanClass());
    val csvReader = new CSVReader(reader, SEPERATOR);
    val strategy = new HeaderColumnNameMappingStrategy<B>();
    strategy.setType(getBeanClass());
    val csvToBean = new CsvToBean<B>();
    return csvToBean.parse(strategy, csvReader);
  }

  public abstract Class<B> getBeanClass();

  @SneakyThrows
  protected AbstractFileDao(String inputFilename) {
    val file = Paths.get(inputFilename).toFile();
    checkState(file.exists(),"The inputFilename [%s] does not exist", file.getAbsolutePath());
    checkState(file.isFile(),"The inputFilename [%s] is not a file" , file.getAbsolutePath());
    this.reader = new FileReader(file);
    this.beans = convert();
    this.reader.close();
    this.requestMap = convertToRequestMap(beans);
    log.info("Done Converting inputFilename {} to DAO ", inputFilename);
  }

  protected abstract R createRequestFromBean(B bean);


  private Map<R, List<B>> convertToRequestMap(List<B> beans){
    return beans.stream()
        .collect(groupingBy(this::createRequestFromBean));
  }

  protected AbstractFileDao(List<B> beans){
    this.beans = beans;
    this.requestMap = convertToRequestMap(beans);
  }

  protected AbstractFileDao(Reader reader) {
    this.reader = reader;
    this.beans = convert();
    this.requestMap = convertToRequestMap(beans);
    log.info("Done Converting Reader to DAO");
  }

  public void store(String filename) throws IOException {
    ObjectPersistance.store(this, filename);
  }

  public List<B> findAll(){
    return ImmutableList.copyOf(getBeans());
  }

  public List<B> find(R inputRequest){
    if (requestMap.containsKey(inputRequest)){
      return requestMap.get(inputRequest);
    } else {
      return requestMap.keySet()
          .stream()
          .filter(r -> r.matches(inputRequest)) //Filter only those requestKeys that match the inputRequest
          .flatMap(
              r -> requestMap.get(r).stream()) // for all the requestKeys left, flaten their bean lists into one list
          //        .distinct() // ensure no repeats
          .collect(toImmutableList());
    }
  }

}
