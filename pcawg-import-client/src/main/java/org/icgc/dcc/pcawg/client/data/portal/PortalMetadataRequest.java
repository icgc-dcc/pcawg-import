package org.icgc.dcc.pcawg.client.data.portal;

import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor(staticName = "newPortalMetadataRequest")
@Data
public class PortalMetadataRequest {

  @NonNull private final PortalFilename portalFilename;

}
