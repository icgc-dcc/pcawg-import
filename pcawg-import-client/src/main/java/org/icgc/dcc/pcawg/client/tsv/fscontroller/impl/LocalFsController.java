package org.icgc.dcc.pcawg.client.tsv.fscontroller.impl;

import lombok.val;
import org.icgc.dcc.pcawg.client.tsv.fscontroller.FsController;
import org.icgc.dcc.pcawg.client.tsv.writer.WriterContext;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;

public final class LocalFsController implements FsController<Path> {

  public static final LocalFsController newLocalFsController(){
    return new LocalFsController();
  }

  @Override public boolean exists(Path path) throws IOException {
    return path.toFile().exists();
  }

  @Override public Writer createWriter(WriterContext<Path> writerContext) throws IOException {
    val filePath = writerContext.getPath();
    val parentPath = filePath.getParent();
    mkdirs(parentPath);
    return new FileWriter(filePath.toFile(), writerContext.isAppend());
  }

  @Override public void deleteIfExists(Path path) throws IOException {
    Files.deleteIfExists(path);
  }

  @Override public void mkdirs(Path path) throws IOException {
    if (path != null && !exists(path)){
      Files.createDirectories(path);
    }
  }
}
