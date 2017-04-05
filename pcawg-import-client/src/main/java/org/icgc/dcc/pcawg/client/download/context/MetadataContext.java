package org.icgc.dcc.pcawg.client.download.context;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.model.portal.PortalMetadata;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;

@Builder
@Value
public class MetadataContext {

  @NonNull
  private final PortalMetadata portalMetadata;

  @NonNull
  private final SampleMetadata sampleMetadata;

}
