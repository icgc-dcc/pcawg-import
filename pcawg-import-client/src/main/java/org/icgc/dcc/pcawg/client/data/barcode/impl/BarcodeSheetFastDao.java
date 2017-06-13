package org.icgc.dcc.pcawg.client.data.barcode.impl;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.BasicDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSheetBean;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.pcawg.client.data.barcode.impl.BarcodeSheetBeanDao.newBarcodeSheetBeanDao;

@Slf4j
public class BarcodeSheetFastDao implements BasicDao<BarcodeSheetBean, BarcodeSearchRequest> {

  public static BarcodeSheetFastDao newBarcodeSheetFastDao(String inputFilename, final boolean hasHeader){
    return new BarcodeSheetFastDao(inputFilename, hasHeader);
  }

  public static BarcodeSheetFastDao newBarcodeSheetFastDao(Reader reader, final boolean hasHeader){
    return new BarcodeSheetFastDao(reader, hasHeader);
  }

  private BufferedReader reader;
  private final boolean hasHeader;

  @Getter(AccessLevel.PROTECTED)
  private List<BarcodeSheetBean> beans;

  private final BarcodeSheetBeanDao internalDao;


  @SneakyThrows
  protected List<BarcodeSheetBean> convert(){
    return reader
        .lines()
        .skip(hasHeader ? 1 : 0)
        .map(BarcodeSheetParser::parseLine)
        .collect(toImmutableList());
  }

  @SneakyThrows
  private BarcodeSheetFastDao(String inputFilename, final boolean hasHeader) {
    this.hasHeader = hasHeader;
    val file = Paths.get(inputFilename).toFile();
    checkState(file.exists(),"The inputFilename [%s] does not exist", file.getAbsolutePath());
    checkState(file.isFile(),"The inputFilename [%s] is not a portal" , file.getAbsolutePath());
    this.reader = new BufferedReader(new FileReader(file));
    this.beans = convert();
    this.reader.close();
    log.info("Done Converting inputFilename {} to DAO ", inputFilename);
    this.internalDao = newBarcodeSheetBeanDao(beans);
  }

  private BarcodeSheetFastDao(Reader reader, final boolean hasHeader) {
    this.hasHeader = hasHeader;
    this.reader = new BufferedReader(reader);
    this.beans = convert();
    log.info("Done Converting Reader to DAO");
    this.internalDao = newBarcodeSheetBeanDao(beans);
  }

  @Override public List<BarcodeSheetBean> find(BarcodeSearchRequest request) {
    return internalDao.find(request);
  }

  @Override public List<BarcodeSheetBean> findAll() {
    return internalDao.findAll();
  }

}
