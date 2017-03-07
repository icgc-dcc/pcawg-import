package org.icgc.dcc.pcawg.client.download.fetcher.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.model.metadata.FileMetaDataContext;
import org.icgc.dcc.pcawg.client.download.fetcher.Fetcher;

import java.io.IOException;
import java.nio.file.Paths;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.pcawg.client.core.model.metadata.FileMetaDataContext.buildFileMetaDataContext;
import static org.icgc.dcc.pcawg.client.download.Portal.getAllFileMetas;

@RequiredArgsConstructor(access = PRIVATE)
public class AllFetcher implements Fetcher {

  private static final String DEFAULT_STORAGE_FILENAME = "target/allFileMetaDatas.dat";

  @NonNull
  private String storageFilename;

  private final boolean forceNewFile;

  public static final AllFetcher newAllFetcher(final String storageFilename, final boolean forceNewFile){
    return new AllFetcher(storageFilename, forceNewFile);
  }

  public static final AllFetcher newAllFetcherDefaultStorageFilename(final boolean forceNewFile){
    return new AllFetcher(DEFAULT_STORAGE_FILENAME, forceNewFile);
  }


  @Override public FileMetaDataContext fetch() throws IOException, ClassNotFoundException {
    val fromFile = Paths.get(storageFilename).toFile();
    val fileExists = fromFile.exists() && fromFile.isFile();

    FileMetaDataContext fileMetaDataContext;
    if (fileExists && !forceNewFile) {
      fileMetaDataContext = FileMetaDataContext.restore(storageFilename);
    } else {
      fileMetaDataContext = buildFileMetaDataContext(getAllFileMetas());
      fileMetaDataContext.store(storageFilename);
    }
    return fileMetaDataContext;
  }
}
