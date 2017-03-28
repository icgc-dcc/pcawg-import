package org.icgc.dcc.pcawg.client.data;

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
import java.nio.file.Paths;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

@Slf4j
public abstract class AbstractFileDao<B, R > {
  private static final char SEPERATOR = '\t';

  private Reader reader;

  @Getter(AccessLevel.PROTECTED)
  private List<B> data;

  @SneakyThrows
  protected List<B> convert(){
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
    this.data = convert();
    this.reader.close();
    log.info("Done Converting inputFilename {} to DAO ", inputFilename);

  }

  protected AbstractFileDao(Reader reader) {
    this.reader = reader;
    this.data = convert();
    log.info("Done Converting Reader to DAO");
  }

  public void store(String filename) throws IOException {
    ObjectPersistance.store(this, filename);
  }

  public abstract List<B> find(R request);

}
