package org.icgc.dcc.pcawg.client.tsv.fscontroller.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.tsv.fscontroller.FsController;
import org.icgc.dcc.pcawg.client.tsv.writer.impl.HadoopWriterContext;
import org.icgc.dcc.pcawg.client.tsv.writer.WriterContext;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;

@RequiredArgsConstructor
public final class HadoopFsControllerAdapter implements FsController<Path> {

  public static final HadoopFsControllerAdapter newHadoopFsControllerAdapter(FsController<org.apache.hadoop.fs.Path> hdfsController){
    return new HadoopFsControllerAdapter(hdfsController);
  }

  @NonNull private final FsController<org.apache.hadoop.fs.Path> hdfsController;

  private static final org.apache.hadoop.fs.Path convertToHadoopPath(Path path){
    return new org.apache.hadoop.fs.Path(path.toString());
  }

  @Override public boolean exists(Path path) throws IOException {
    return hdfsController.exists(convertToHadoopPath(path));
  }

  @Override public Writer createWriter(WriterContext<Path> writerContext) throws IOException {
    val hdfsWriterContext = HadoopWriterContext.builder()
        .append(writerContext.isAppend())
        .path(convertToHadoopPath(writerContext.getPath()))
        .build();
    return hdfsController.createWriter(hdfsWriterContext);
  }

  @Override public void deleteIfExists(Path path) throws IOException {
    hdfsController.deleteIfExists(convertToHadoopPath(path));
  }

  @Override public void mkdirs(Path path) throws IOException {
    hdfsController.mkdirs(convertToHadoopPath(path));
  }
}
