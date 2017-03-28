package org.icgc.dcc.pcawg.client.data.sample;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.pcawg.client.data.sample.SampleBeanDao.newSampleBeanDao;

@Slf4j
public class SampleFastDao implements SampleDao<SampleBean, SampleSearchRequest> {

  public static SampleFastDao newSampleFastDao(String inputFilename, final boolean hasHeader){
    return new SampleFastDao(inputFilename, hasHeader);
  }

  public static SampleFastDao newSampleFastDao(Reader reader, final boolean hasHeader){
    return new SampleFastDao(reader, hasHeader);
  }

  private BufferedReader reader;
  private final boolean hasHeader;

  @Getter(AccessLevel.PROTECTED)
  private List<SampleBean> beans;

  private final SampleBeanDao internalDao;

  @SneakyThrows
  protected List<SampleBean> convert(){
    return reader
        .lines()
        .skip(hasHeader ? 1 : 0)
        .map(SampleSheetParser::parseLine)
        .collect(toImmutableList());
  }


  @SneakyThrows
  private SampleFastDao(String inputFilename, final boolean hasHeader) {
    this.hasHeader = hasHeader;
    val file = Paths.get(inputFilename).toFile();
    checkState(file.exists(),"The inputFilename [%s] does not exist", file.getAbsolutePath());
    checkState(file.isFile(),"The inputFilename [%s] is not a file" , file.getAbsolutePath());
    this.reader = new BufferedReader(new FileReader(file));
    this.beans = convert();
    this.reader.close();
    log.info("Done Converting inputFilename {} to DAO ", inputFilename);
    this.internalDao =  newSampleBeanDao(beans);
  }

  private SampleFastDao (Reader reader, final boolean hasHeader) {
    this.hasHeader = hasHeader;
    this.reader = new BufferedReader(reader);
    this.beans = convert();
    log.info("Done Converting Reader to DAO");
    this.internalDao =  newSampleBeanDao(beans);
  }


  @Override
  public List<SampleBean> find(SampleSearchRequest request) {
    return internalDao.find(request);
  }

  @Override
  public Optional<SampleBean> findFirstAliquotId(String aliquotId) {
    return internalDao.findFirstAliquotId(aliquotId);
  }

  @Override
  public List<SampleBean> findAliquotId(String aliquotId) {
    return internalDao.findAliquotId(aliquotId);
  }

  @Override
  public Optional<SampleBean> findFirstDonorUniqueId(String donorUniqueId) {
    return internalDao.findFirstDonorUniqueId(donorUniqueId);
  }

  @Override
  public List<SampleBean> findDonorUniqueId(String donorUniqueId) {
    return internalDao.findDonorUniqueId(donorUniqueId);
  }
}
