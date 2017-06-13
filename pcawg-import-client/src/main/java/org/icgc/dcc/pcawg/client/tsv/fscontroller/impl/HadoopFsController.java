package org.icgc.dcc.pcawg.client.tsv.fscontroller.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.pcawg.client.tsv.fscontroller.FsController;
import org.icgc.dcc.pcawg.client.tsv.writer.WriterContext;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

@RequiredArgsConstructor
@Slf4j
public final class HadoopFsController implements FsController<Path> {


  /**
   * Constants
   */
  private static final String FS_PARAM_NAME = "fs.defaultFS";
  private static final String HDFS = "hdfs";
  private static final boolean DELETE_RECURSIVELY = true;

  /**
   * Static Functions
   */
  public static HadoopFsController newHadoopFsController(String hostname, String port){
    return new HadoopFsController(createFileSystem(hostname, port));
  }

  private static Configuration createConfiguration(String hostname, String port){
    val conf = new Configuration();
    val baseUrl = createUrl(HDFS, hostname, port);
    conf.set(FS_PARAM_NAME, baseUrl);
    return conf;
  }

  private static String createUrl(String fileSystemName, String hostname, String port){
    return fileSystemName+"://"+hostname+":"+port;
  }

  @SneakyThrows
  private static FileSystem createFileSystem(String hostname, String port){
    val conf = createConfiguration(hostname, port);
    return FileSystem.get(conf);
  }


  /**
   * Configuration
   */
  @NonNull private final FileSystem fs;

  @Override public boolean exists(Path path) throws IOException {
    return fs.exists(path);
  }

  @Override public Writer createWriter(WriterContext<Path> writerContext) throws IOException {
    val path = writerContext.getPath();
    val append = writerContext.isAppend();
    val os = createNewOutputStream(path, append);
    return new BufferedWriter(new OutputStreamWriter(os));
  }

  @SneakyThrows
  private OutputStream createNewOutputStream(Path file, boolean append){
    val parent = file.getParent();

    val parentDirExists = fs.exists(parent);
    val fileExists = fs.exists(file);

    if (! parentDirExists ){
      log.info("The parent dir [{}] doesnt exist, will create...  ", parent.toUri().toString() );
      fs.mkdirs(parent);
      return fs.create(file);
    } else if(!fileExists){
      return fs.create(file);
    } else {
      return append  ? fs.append(file) : fs.create(file);
    }
  }

  @Override public void deleteIfExists(Path path) throws IOException {
    if (exists(path)){
      fs.delete(path, DELETE_RECURSIVELY);
    }
  }

  @Override public void mkdirs(Path path) throws IOException {
    if (path != null && !exists(path)){
      fs.mkdirs(path);
    }
  }

}
