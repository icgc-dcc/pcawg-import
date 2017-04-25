package org.icgc.dcc.pcawg.client.data.portal;

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.pcawg.client.model.portal.PortalFilename;

@RequiredArgsConstructor
@Data
public class PortalMetadataRequest {

  public static PortalMetadataRequest newPortalMetadataRequest(PortalFilename portalFilename) {
    return new PortalMetadataRequest(portalFilename);
  }

  @NonNull private final PortalFilename portalFilename;

}
