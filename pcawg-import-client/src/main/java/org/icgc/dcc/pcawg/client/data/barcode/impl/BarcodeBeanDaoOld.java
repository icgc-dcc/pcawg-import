package org.icgc.dcc.pcawg.client.data.barcode.impl;

import com.google.common.collect.ImmutableList;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeDao;
import org.icgc.dcc.pcawg.client.model.beans.BarcodeBean;
import org.icgc.dcc.pcawg.client.utils.ObjectPersistance;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

public class BarcodeBeanDaoOld implements Serializable, BarcodeDao<BarcodeBean, String> {

  public static final long serialVersionUID = 1490628681L;
  private static final char SEPERATOR = '\t';

  private static List<BarcodeBean> convert(Reader reader){
    val csvReader = new CSVReader(reader, SEPERATOR);
    val strategy = new HeaderColumnNameMappingStrategy<BarcodeBean>();
    strategy.setType(BarcodeBean.class);
    val csvToBean = new CsvToBean<BarcodeBean>();
    return csvToBean.parse(strategy, csvReader);
  }

  public static BarcodeBeanDaoOld newBarcodeBeanDaoOld(String inputFilename){
    return new BarcodeBeanDaoOld(inputFilename);
  }

  public static BarcodeBeanDaoOld newBarcodeBeanDaoOld(Reader reader){
    return new BarcodeBeanDaoOld(reader);
  }

  @SneakyThrows
  public static BarcodeBeanDaoOld restoreBarcodeBeanDaoOld(String storedBarcodeDaoFilename){
    return (BarcodeBeanDaoOld) ObjectPersistance.restore(storedBarcodeDaoFilename);
  }

  private final Reader reader;

  private List<BarcodeBean> beans;

  @Override
  public List<BarcodeBean> find(String uuid){
    return beans.stream()
        .filter(b -> b.getUuid().equals(uuid))
        .collect(toImmutableList());
  }

  @SneakyThrows
  private BarcodeBeanDaoOld(String inputFilename) {
    val file = Paths.get(inputFilename).toFile();
    checkState(file.exists(),"The inputFilename [%s] does not exist", file.getAbsolutePath());
    checkState(file.isFile(),"The inputFilename [%s] is not a file" , file.getAbsolutePath());
    this.reader = new FileReader(file);
    this.beans = convert(reader);
    this.reader.close();
  }

  private BarcodeBeanDaoOld(Reader reader) {
    this.reader = reader;
    this.beans = convert(reader);
  }

  public void store(String filename) throws IOException{
    ObjectPersistance.store(this, filename);
  }

  @Override public List<BarcodeBean> findAll() {
    return ImmutableList.copyOf(beans);
  }
}
