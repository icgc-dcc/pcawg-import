package org.icgc.dcc.pcawg.client.core;

import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.download.Storage;

import java.nio.file.Path;

import static org.icgc.dcc.pcawg.client.download.LocalStorage.newLocalStorage;
import static org.icgc.dcc.pcawg.client.download.PortalStorage.newPortalStorage;

@RequiredArgsConstructor
@Builder
public class StorageFactory {

  @NonNull  private final Path outputVcfDir;
  private final boolean useCollab;
  private final boolean bypassMD5Check;
  @NonNull private final String token;
  private final boolean persistVcfDownloads;

  public Storage getStorage(String dccProjectCode){
    if(useCollab){
      val vcfDownloadDirectory = outputVcfDir.resolve(dccProjectCode);
      return newPortalStorage(persistVcfDownloads, vcfDownloadDirectory.toString(), bypassMD5Check, token);
    } else {
      return newLocalStorage(outputVcfDir,bypassMD5Check);
    }
  }


}
