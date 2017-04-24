package org.icgc.dcc.pcawg.client.core.transformer.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

@Value
@RequiredArgsConstructor(staticName = "newDccTransformerContext")
public class DccTransformerContext<T> {

  @NonNull private final WorkflowTypes workflowTypes;

  @NonNull private final T object;

}
