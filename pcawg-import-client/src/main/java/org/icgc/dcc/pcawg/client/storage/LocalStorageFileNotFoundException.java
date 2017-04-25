package org.icgc.dcc.pcawg.client.storage;

import lombok.Getter;
import lombok.NonNull;
import org.icgc.dcc.pcawg.client.model.portal.PortalMetadata;

public class LocalStorageFileNotFoundException extends RuntimeException{

  @Getter private final PortalMetadata portalMetadata;

  public LocalStorageFileNotFoundException(String message, @NonNull PortalMetadata portalMetadata) {
    super(message);
    this.portalMetadata = portalMetadata;
  }

}
