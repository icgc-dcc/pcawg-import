package org.icgc.dcc.pcawg.client.data.barcode;

import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import lombok.SneakyThrows;
import lombok.val;

import java.io.Closeable;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

public class BarcodeDao implements Closeable {

  private static final char SEPERATOR = '\t';

  private static List<BarcodeBean> convert(Reader reader){
    val csvReader = new CSVReader(reader, SEPERATOR);
    val strategy = new HeaderColumnNameMappingStrategy<BarcodeBean>();
    strategy.setType(BarcodeBean.class);
    val csvToBean = new CsvToBean<BarcodeBean>();
    return csvToBean.parse(strategy, csvReader);
  }

  private final Reader reader;

  private List<BarcodeBean> data;

  public List<BarcodeBean> find(String uuid){
    return data.stream()
        .filter(b -> b.getUuid().equals(uuid))
        .collect(toImmutableList());
  }

  @SneakyThrows
  public BarcodeDao(String inputFilename) {
    val file = Paths.get(inputFilename).toFile();
    checkState(file.exists(),"The inputFilename [%s] does not exist", file.getAbsolutePath());
    checkState(file.isFile(),"The inputFilename [%s] is not a file" , file.getAbsolutePath());
    this.reader = new FileReader(file);
    this.data = convert(reader);
  }

  public BarcodeDao(Reader reader) {
    this.reader = reader;
    this.data = convert(reader);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

}
