package org.icgc.dcc.pcawg.client.core.transformer.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

@Value
@RequiredArgsConstructor
public class DccTransformerContext<T> {

  public static <T> DccTransformerContext<T> newDccTransformerContext(WorkflowTypes workflowTypes, T object) {
    return new DccTransformerContext<T>(workflowTypes, object);
  }

  @NonNull private final WorkflowTypes workflowTypes;

  @NonNull private final T object;

}
