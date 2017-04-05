package org.icgc.dcc.pcawg.client.core.transformer;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.transformer.impl.BaseTransformer;
import org.icgc.dcc.pcawg.client.core.writer.FileWriterContext;
import org.icgc.dcc.pcawg.client.core.writer.HdfsFileWriter;
import org.icgc.dcc.pcawg.client.core.writer.LocalFileWriter;
import org.icgc.dcc.pcawg.client.tsv.TSVConverter;

import java.io.IOException;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.pcawg.client.core.writer.HdfsFileWriter.newDefaultHdfsFileWriter;
import static org.icgc.dcc.pcawg.client.core.writer.LocalFileWriter.newDefaultLocalFileWriter;

@RequiredArgsConstructor(access = PRIVATE)
@Slf4j
public final class TransformerFactory<T> {

  public static final <T> TransformerFactory<T> newTransformerFactory(TSVConverter<T> tsvConverter, final boolean useHdfs){
    return new TransformerFactory<T>(tsvConverter, useHdfs);
  }

  @NonNull
  private final TSVConverter<T> tsvConverter;

  private final boolean useHdfs;

  public final BaseTransformer<T> getTransformer(FileWriterContext context){
    if (useHdfs){
      return newHdfsTransformer(context);
    } else {
      return newLocalFileTransformer(context);
    }
  }

  @SneakyThrows
  private BaseTransformer<T> newLocalFileTransformer(FileWriterContext context){
    val appendMode = context.isAppend();
    val localFileWriter = createDefaultLocalFileWriter(context);
    val doesFileExist = localFileWriter.isFileExistedPreviously();
    val writeHeaderLineInitially =  ! appendMode || !doesFileExist;
    return BaseTransformer.newBaseTransformer(tsvConverter, localFileWriter, writeHeaderLineInitially);
  }

  @SneakyThrows
  private BaseTransformer<T> newHdfsTransformer(FileWriterContext context){
    val appendMode = context.isAppend();
    val hdfsFileWriter= createHdfsFileWriter(context);
    val doesFileExist = hdfsFileWriter.isFileExistedPreviously();
    val writeHeaderLineInitially =  ! appendMode || !doesFileExist;
    return BaseTransformer.newBaseTransformer(tsvConverter, hdfsFileWriter, writeHeaderLineInitially);
  }

  private static LocalFileWriter createDefaultLocalFileWriter(FileWriterContext context) throws IOException {
    return newDefaultLocalFileWriter(context.getOutputFilename(),
        context.isAppend());
  }

  private static HdfsFileWriter createHdfsFileWriter(FileWriterContext context) throws IOException {
    return newDefaultHdfsFileWriter(context.getHostname(),
        context.getPort(),
        context.getOutputFilename(),
        context.isAppend() );
  }

}
