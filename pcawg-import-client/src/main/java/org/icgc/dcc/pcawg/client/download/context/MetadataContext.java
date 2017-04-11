package org.icgc.dcc.pcawg.client.download.context;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.model.portal.PortalMetadata;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;

import java.io.Serializable;

@Builder
@Value
public class MetadataContext implements Serializable {

  public static final long serialVersionUID = 1491936541L;

  @NonNull
  private final PortalMetadata portalMetadata;

  @NonNull
  private final SampleMetadata sampleMetadata;

}
