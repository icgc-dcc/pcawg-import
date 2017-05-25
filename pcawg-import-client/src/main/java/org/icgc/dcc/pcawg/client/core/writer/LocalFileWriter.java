package org.icgc.dcc.pcawg.client.core.writer;

import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Extends FileWriter to create all parent directories first, before create Writer object
 */
public class LocalFileWriter extends Writer {

  private static final boolean CREATE_DIRS_IF_DNE = true;

  public static LocalFileWriter newLocalFileWriter(String filename, boolean append, boolean createNonExistentDirectories) throws IOException{
    val fileAlreadyExists= Paths.get(filename).toFile().exists();
    return new LocalFileWriter(filename, append, createNonExistentDirectories, fileAlreadyExists);
  }

  public static LocalFileWriter newDefaultLocalFileWriter(String filename, boolean append) throws IOException{
    return newLocalFileWriter(filename, append, CREATE_DIRS_IF_DNE);
  }

  @Getter
  private final String filename;


  @Getter
  private final boolean append;

  @Getter
  private final boolean createNonExistentDirectories;

  @Getter
  private final boolean fileExistedPreviously;

  private boolean hasAtLeastOneWrite = false;

  private final FileWriter internalFileWriter;

  private LocalFileWriter(@NonNull String filename, final boolean append,
      final boolean createNonExistentDirectories, final boolean fileExistedPreviously) throws IOException {
    createParentDirectoriesFirst(filename, createNonExistentDirectories);
    this.internalFileWriter = new FileWriter(filename, append);
    this.filename = filename;
    this.append = append;
    this.fileExistedPreviously = fileExistedPreviously;
    this.createNonExistentDirectories = createNonExistentDirectories;
  }

  @Override
  public void write(char cbuf[], int off, int len) throws IOException {
    hasAtLeastOneWrite = true;
    internalFileWriter.write(cbuf,off,len);
  }

  @Override
  public void flush() throws IOException {
    internalFileWriter.flush();
  }

  // If never wrote to file, and never existed previously, then remove the file, since creating a new FileWriter creates a file even if you never write to it
  @Override
  public void close() throws IOException{
    internalFileWriter.close();
    if (!hasAtLeastOneWrite && !isFileExistedPreviously()){
      Files.deleteIfExists(Paths.get(filename));
    }
  }

  @SneakyThrows
  private static String createParentDirectoriesFirst(String filename, boolean createNonExistentDirectories){

    if(createNonExistentDirectories){
      val filePath = Paths.get(filename);
      val parent = filePath.toFile().getParentFile();
      if (parent != null){
        val parentPath = Paths.get(parent.toURI());
        if (! parent.exists()){
          Files.createDirectories(parentPath);
        }
      }
    }
    return filename;
  }
}
