package org.icgc.dcc.pcawg.client.core.writer;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

@Slf4j
public class HdfsFileWriter extends Writer {

  private static final String FS_PARAM_NAME = "fs.defaultFS";
  private static final String HDFS = "hdfs";
  private static final boolean DEFAULT_CREATE_DIRS_IF_DNE = true;
  private static final boolean DELETE_RECURSIVELY = true;

  public static HdfsFileWriter newHdfsFileWriter(String hostname, String port, String outputFilename,
      final boolean append, final boolean createNonExistentDirectories) throws IOException {
    return new HdfsFileWriter(hostname, port, outputFilename, append, createNonExistentDirectories);
  }

  public static HdfsFileWriter newDefaultHdfsFileWriter(String hostname, String port, String outputFilename,
      final boolean append) throws IOException {
    return newHdfsFileWriter(hostname, port, outputFilename, append, DEFAULT_CREATE_DIRS_IF_DNE);
  }

  private final Writer internalWriter;

  @Getter
  private final boolean append;

  private final FileSystem hdfs;
  private final Path file;

  /**
   * If set to true, when writing to a file whose parent directory doesnt exist, it will automatically create the directories, and then open the new file.
   */
  @Getter
  private final boolean createNonExistantDirectories;


  /**
   * State
   */
  @Getter
  @Setter(AccessLevel.PRIVATE)
  private boolean fileExistedPreviously;

  private boolean hasAtLeastOneWrite;

  private static String createUrl(String fileSystemName, String hostname, String port){
    return fileSystemName+"://"+hostname+":"+port;
  }

  private static Configuration createConfiguration(String hostname, String port){
    val conf = new Configuration();
    val baseUrl = createUrl(HDFS, hostname, port);
    conf.set(FS_PARAM_NAME, baseUrl);
    return conf;
  }

  @SneakyThrows
  private static FileSystem createFileSystem(String hostname, String port){
    val conf = createConfiguration(hostname, port);
    return FileSystem.get(conf);
  }

  //TODO: clean this up of all logs
  @SneakyThrows
  private OutputStream createNewOutputStream(FileSystem fs, Path file, boolean append){
    val parent = file.getParent();
    val parentDirExists = fs.exists(parent);
    setFileExistedPreviously(fs.exists(file));
    if (isCreateNonExistantDirectories() && ! isFileExistedPreviously() && ! parentDirExists ){
      log.info("The parent dir [{}] doesnt exist, will create...  ", parent.toUri().toString() );
      fs.mkdirs(parent);
    }

    // Cannot append to a file that doesnt exist
    boolean chooseAppend = isFileExistedPreviously() && append;
    return chooseAppend  ? fs.append(file) : fs.create(file);
  }

  private HdfsFileWriter(String hostname, String port, String outputFilename, final boolean append, final boolean createNonExistentDirectories) throws IOException {
    this.file = new Path(outputFilename);
    this.hdfs = createFileSystem(hostname,port);
    log.info("Connecting to: {}", hdfs.getConf().get(FS_PARAM_NAME));
    log.info("Outputting to: {} ", outputFilename);
    val os = createNewOutputStream(hdfs, file, append);
    this.internalWriter = new BufferedWriter(new OutputStreamWriter(os));
    this.append = append;
    this.createNonExistantDirectories = createNonExistentDirectories;
    this.hasAtLeastOneWrite = isFileExistedPreviously();
  }


  @Override
  public void write(char[] cbuf, int off, int len) throws IOException {
    this.hasAtLeastOneWrite = true;
    internalWriter.write(cbuf,off, len);
  }

  @Override
  public void flush() throws IOException {
    internalWriter.flush();
  }
  @Override
  public void close() throws IOException {
    internalWriter.close();
    if (! hasAtLeastOneWrite && hdfs.exists(file)){
      hdfs.delete(file, DELETE_RECURSIVELY);
    }
  }
}
