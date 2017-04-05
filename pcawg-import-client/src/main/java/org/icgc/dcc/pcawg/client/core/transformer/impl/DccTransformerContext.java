package org.icgc.dcc.pcawg.client.core.transformer.impl;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

@Builder
@Value
public final class DccTransformerContext<T> {

  @NonNull
  private final WorkflowTypes workflowTypes;

  @NonNull
  private final T object;

}
