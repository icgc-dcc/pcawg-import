package org.icgc.dcc.pcawg.client.storage;

import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.nio.file.Path;

import static org.icgc.dcc.pcawg.client.storage.impl.LocalStorage.newLocalStorage;
import static org.icgc.dcc.pcawg.client.storage.impl.PortalStorage.newPortalStorage;

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
