package org.icgc.dcc.pcawg.client.core;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.icgc.dcc.pcawg.client.tsv.TSVConverter;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;

@Slf4j
public class Transformer<T> implements Closeable{

  private static final String NEWLINE = "\n";

  private final TSVConverter<T> tsvConverter;

  private final Writer writer;

  @Getter
  private boolean writeHeader;

  public static <T> Transformer<T> newTransformer( final TSVConverter<T> tsvConverter, final Writer writer, final boolean isNewFile){
    return new Transformer<T>(tsvConverter, writer, isNewFile);
  }

  @SneakyThrows
  private Transformer( final TSVConverter<T> tsvConverter, final Writer writer, final boolean writeHeader){
    this.tsvConverter = tsvConverter;
    this.writeHeader = writeHeader;
    this.writer = writer;
  }

  @SneakyThrows
  public void transform(T t){
    if(writeHeader){
      writer.write(tsvConverter.toTSVHeader()+NEWLINE);
      writeHeader = false;
    }
    writer.write(tsvConverter.toTSVData(t)+ NEWLINE);
  }

  @Override
  public void close() throws IOException {
      writer.close();
  }

}
