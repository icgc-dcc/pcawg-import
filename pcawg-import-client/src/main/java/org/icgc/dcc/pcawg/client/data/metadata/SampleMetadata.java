package org.icgc.dcc.pcawg.client.data.metadata;

import org.icgc.dcc.pcawg.client.model.types.WorkflowTypes;

public interface SampleMetadata {

  String getAliquotId();

  WorkflowTypes getWorkflowType();

  boolean isUsProject();

  String getAnalyzedSampleId();

  String getDccProjectCode();

  String getMatchedSampleId();

  String getAnalyzedFileId();

  String getMatchedFileId();
}
