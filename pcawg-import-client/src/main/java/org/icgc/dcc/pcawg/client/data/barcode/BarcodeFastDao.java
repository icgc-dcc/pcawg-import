package org.icgc.dcc.pcawg.client.data.barcode;

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

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.pcawg.client.data.barcode.BarcodeBeanDao.newBarcodeBeanDao;

@Slf4j
public class BarcodeFastDao implements BarcodeDao<BarcodeBean, String> {

  public static BarcodeFastDao newBarcodeFastDao(String inputFilename, final boolean hasHeader){
    return new BarcodeFastDao(inputFilename, hasHeader);
  }

  public static BarcodeFastDao newBarcodeFastDao(Reader reader, final boolean hasHeader){
    return new BarcodeFastDao(reader, hasHeader);
  }

  private BufferedReader reader;
  private final boolean hasHeader;

  @Getter(AccessLevel.PROTECTED)
  private List<BarcodeBean> beans;

  private final BarcodeBeanDao internalDao;

  @SneakyThrows
  protected List<BarcodeBean> convert(){
    return reader
        .lines()
        .skip(hasHeader ? 1 : 0)
        .map(BarcodeSheetParser::parseLine)
        .collect(toImmutableList());
  }

  @SneakyThrows
  private BarcodeFastDao(String inputFilename, final boolean hasHeader) {
    this.hasHeader = hasHeader;
    val file = Paths.get(inputFilename).toFile();
    checkState(file.exists(),"The inputFilename [%s] does not exist", file.getAbsolutePath());
    checkState(file.isFile(),"The inputFilename [%s] is not a file" , file.getAbsolutePath());
    this.reader = new BufferedReader(new FileReader(file));
    this.beans = convert();
    this.reader.close();
    log.info("Done Converting inputFilename {} to DAO ", inputFilename);
    this.internalDao = newBarcodeBeanDao(beans);
  }

  private BarcodeFastDao (Reader reader, final boolean hasHeader) {
    this.hasHeader = hasHeader;
    this.reader = new BufferedReader(reader);
    this.beans = convert();
    log.info("Done Converting Reader to DAO");
    this.internalDao = newBarcodeBeanDao(beans);
  }

  @Override
  public List<BarcodeBean> find(String uuid) {
    return internalDao.find(uuid);
  }
}
