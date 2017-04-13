package org.icgc.dcc.pcawg.client.data.barcode.impl;

import com.google.common.collect.ImmutableList;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.BasicDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSheetBean;
import org.icgc.dcc.pcawg.client.utils.persistance.ObjectPersistance;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

public class BarcodeSheetDaoOld implements Serializable, BasicDao<BarcodeSheetBean, String> {

  public static final long serialVersionUID = 1490628681L;
  private static final char SEPERATOR = '\t';

  private static List<BarcodeSheetBean> convert(Reader reader){
    val csvReader = new CSVReader(reader, SEPERATOR);
    val strategy = new HeaderColumnNameMappingStrategy<BarcodeSheetBean>();
    strategy.setType(BarcodeSheetBean.class);
    val csvToBean = new CsvToBean<BarcodeSheetBean>();
    return csvToBean.parse(strategy, csvReader);
  }

  public static BarcodeSheetDaoOld newBarcodeBeanDaoOld(String inputFilename){
    return new BarcodeSheetDaoOld(inputFilename);
  }

  public static BarcodeSheetDaoOld newBarcodeBeanDaoOld(Reader reader){
    return new BarcodeSheetDaoOld(reader);
  }

  @SneakyThrows
  public static BarcodeSheetDaoOld restoreBarcodeBeanDaoOld(String storedBarcodeDaoFilename){
    return (BarcodeSheetDaoOld) ObjectPersistance.restore(storedBarcodeDaoFilename);
  }

  private final Reader reader;

  private List<BarcodeSheetBean> beans;

  @Override
  public List<BarcodeSheetBean> find(String uuid){
    return beans.stream()
        .filter(b -> b.getUuid().equals(uuid))
        .collect(toImmutableList());
  }

  @SneakyThrows
  private BarcodeSheetDaoOld(String inputFilename) {
    val file = Paths.get(inputFilename).toFile();
    checkState(file.exists(),"The inputFilename [%s] does not exist", file.getAbsolutePath());
    checkState(file.isFile(),"The inputFilename [%s] is not a portal" , file.getAbsolutePath());
    this.reader = new FileReader(file);
    this.beans = convert(reader);
    this.reader.close();
  }

  private BarcodeSheetDaoOld(Reader reader) {
    this.reader = reader;
    this.beans = convert(reader);
  }

  public void store(String filename) throws IOException{
    ObjectPersistance.store(this, filename);
  }

  @Override public List<BarcodeSheetBean> findAll() {
    return ImmutableList.copyOf(beans);
  }
}
