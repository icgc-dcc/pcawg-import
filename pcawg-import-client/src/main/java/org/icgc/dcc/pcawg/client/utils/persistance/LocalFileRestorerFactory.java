package org.icgc.dcc.pcawg.client.utils.persistance;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorer.newLocalFileRestorer;

public class LocalFileRestorerFactory {

  public static final LocalFileRestorerFactory newFileRestorerFactory(Path outputDir){
    return new LocalFileRestorerFactory(outputDir);
  }
  public static final LocalFileRestorerFactory newFileRestorerFactory(String outputDirname){
    return newFileRestorerFactory(Paths.get(outputDirname));
  }

  @NonNull private final Path outputDir;

  @SneakyThrows
  public LocalFileRestorerFactory(Path outputDir){
    this.outputDir = outputDir;
    initDir(outputDir);
  }

  private static void initDir(Path outputDir) throws IOException {
    if (!outputDir.toFile().exists()){
      Files.createDirectories(outputDir);
    }
  }

  public <T extends Serializable> FileRestorer<Path, T>  createFileRestorer(String filename){
    val path = outputDir.resolve(filename);
    return newLocalFileRestorer(path);
  }

}
