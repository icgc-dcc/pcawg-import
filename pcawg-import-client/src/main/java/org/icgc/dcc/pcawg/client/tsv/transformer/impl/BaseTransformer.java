package org.icgc.dcc.pcawg.client.tsv.transformer.impl;

import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.icgc.dcc.pcawg.client.tsv.transformer.Transformer;
import org.icgc.dcc.pcawg.client.tsv.converter.TSVConverter;

import java.io.IOException;
import java.io.Writer;

/**
 * Base implementation for Transformer. Transforms input T data via tsv converter to string, and writes to writer
 * @param <T> input data type
 */
@Slf4j
public final class BaseTransformer<T> implements Transformer<T> {

  private static final String NEWLINE = "\n";

  private final TSVConverter<T> tsvConverter;

  private final Writer writer;

  @Getter
  private boolean writeHeader;

  public static <T> BaseTransformer<T> newBaseTransformer( final TSVConverter<T> tsvConverter, final Writer writer, final boolean isNewFile){
    return new BaseTransformer<T>(tsvConverter, writer, isNewFile);
  }

  @SneakyThrows
  private BaseTransformer( final TSVConverter<T> tsvConverter, final Writer writer, final boolean writeHeader){
    this.tsvConverter = tsvConverter;
    this.writeHeader = writeHeader;
    this.writer = writer;
  }

  @Override @SneakyThrows
  public void transform(T t){
    if(writeHeader){
      writer.write(tsvConverter.toTSVHeader()+NEWLINE);
      writeHeader = false;
    }
    writer.write(tsvConverter.toTSVData(t)+ NEWLINE);
  }

  @Override
  public void flush() throws IOException {
    writer.flush();
  }

  @Override
  public void close() throws IOException {
      writer.close();
  }

}
