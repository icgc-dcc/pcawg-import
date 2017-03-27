package org.icgc.dcc.pcawg.client.data.ega;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class EgaSearchRequest {

  public static EgaSearchRequest fromEgaBean(EgaBean b) {
    return new EgaSearchRequest(b.getIcgc_project_code(), b.getSubmitter_sample_id());
  }

  @NonNull private final String dccProjectCode;
  @NonNull private final String submitterSampleId;

}
