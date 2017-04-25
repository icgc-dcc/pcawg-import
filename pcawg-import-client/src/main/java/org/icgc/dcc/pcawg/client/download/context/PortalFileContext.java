package org.icgc.dcc.pcawg.client.download.context;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.core.model.portal.PortalMetadata;

import java.io.File;

@Value
@Builder
public class PortalFileContext {

  @NonNull
  private final File file;

  @NonNull
  private final PortalMetadata portalMetadata;

}
