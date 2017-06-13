package org.icgc.dcc.pcawg.client.data.sample.impl;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetSearchRequest;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetBean;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.pcawg.client.data.sample.impl.SampleSheetBeanDao.newSampleSheetBeanDao;

@Slf4j
public class SampleSheetFastDao implements SampleSheetDao<SampleSheetBean, SampleSheetSearchRequest> {

  public static SampleSheetFastDao newSampleSheetFastDao(String inputFilename, final boolean hasHeader){
    return new SampleSheetFastDao(inputFilename, hasHeader);
  }

  public static SampleSheetFastDao newSampleSheetFastDao(Reader reader, final boolean hasHeader){
    return new SampleSheetFastDao(reader, hasHeader);
  }

  private BufferedReader reader;
  private final boolean hasHeader;

  @Getter(AccessLevel.PROTECTED)
  private List<SampleSheetBean> beans;

  private final SampleSheetBeanDao internalDao;

  @SneakyThrows
  protected List<SampleSheetBean> convert(){
    return reader
        .lines()
        .skip(hasHeader ? 1 : 0)
        .map(SampleSheetParser::parseLine)
        .collect(toImmutableList());
  }


  @SneakyThrows
  private SampleSheetFastDao(String inputFilename, final boolean hasHeader) {
    this.hasHeader = hasHeader;
    val file = Paths.get(inputFilename).toFile();
    checkState(file.exists(),"The inputFilename [%s] does not exist", file.getAbsolutePath());
    checkState(file.isFile(),"The inputFilename [%s] is not a file" , file.getAbsolutePath());
    this.reader = new BufferedReader(new FileReader(file));
    this.beans = convert();
    this.reader.close();
    log.info("Done Converting inputFilename {} to DAO ", inputFilename);
    this.internalDao =  newSampleSheetBeanDao(beans);
  }

  private SampleSheetFastDao(Reader reader, final boolean hasHeader) {
    this.hasHeader = hasHeader;
    this.reader = new BufferedReader(reader);
    this.beans = convert();
    log.info("Done Converting Reader to DAO");
    this.internalDao =  newSampleSheetBeanDao(beans);
  }


  @Override
  public List<SampleSheetBean> find(SampleSheetSearchRequest request) {
    return internalDao.find(request);
  }

  @Override
  public Optional<SampleSheetBean> findFirstAliquotId(String aliquotId) {
    return internalDao.findFirstAliquotId(aliquotId);
  }

  @Override
  public List<SampleSheetBean> findAliquotId(String aliquotId) {
    return internalDao.findAliquotId(aliquotId);
  }

  @Override
  public Optional<SampleSheetBean> findFirstDonorUniqueId(String donorUniqueId) {
    return internalDao.findFirstDonorUniqueId(donorUniqueId);
  }

  @Override
  public List<SampleSheetBean> findDonorUniqueId(String donorUniqueId) {
    return internalDao.findDonorUniqueId(donorUniqueId);
  }

  @Override public List<SampleSheetBean> findAll() {
    return internalDao.findAll();
  }
}
