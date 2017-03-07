package org.icgc.dcc.pcawg.client.download.fetcher.decorators;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.model.metadata.FileMetaDataContext;
import org.icgc.dcc.pcawg.client.download.fetcher.Fetcher;

/**
 * TODO: ditch Fetcher dependancy, and directly depend on FileMetaDataContext, and just make this decorator a decorator for FileMetaDataContext
 */
@RequiredArgsConstructor
public class MaxFileSizeFetcherDecorator implements Fetcher {

  public static MaxFileSizeFetcherDecorator newMaxFileSizeFetcherDecorator(final Fetcher fetcher, final long maxFileSizeBytes){
    return new MaxFileSizeFetcherDecorator(fetcher, maxFileSizeBytes);
  }

  @NonNull
  private final Fetcher fetcher;

  private final long maxFileSizeBytes;

  @Override
  @SneakyThrows
  public FileMetaDataContext fetch() {
    val fileMetaDataContext = fetcher.fetch();
    return fileMetaDataContext.filter(f -> f.getFileSize() < maxFileSizeBytes);
  }
}
