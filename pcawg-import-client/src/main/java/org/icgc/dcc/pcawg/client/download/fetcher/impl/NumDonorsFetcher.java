
package org.icgc.dcc.pcawg.client.download.fetcher.impl;

import lombok.RequiredArgsConstructor;
import org.icgc.dcc.pcawg.client.core.model.metadata.FileMetaDataContext;
import org.icgc.dcc.pcawg.client.download.fetcher.Fetcher;
import org.icgc.dcc.pcawg.client.download.Portal;

import java.io.IOException;

import static org.icgc.dcc.pcawg.client.core.model.metadata.FileMetaDataContext.buildFileMetaDataContext;

@RequiredArgsConstructor
public class NumDonorsFetcher implements Fetcher {

  public static NumDonorsFetcher newNumDonorsFetcher(final int numDonors){
    return new NumDonorsFetcher(numDonors);
  }

  private final int numDonors;

  @Override
  public FileMetaDataContext fetch() throws IOException, ClassNotFoundException {
    return buildFileMetaDataContext(Portal.getFileMetasForNumDonors(numDonors));
  }
}
