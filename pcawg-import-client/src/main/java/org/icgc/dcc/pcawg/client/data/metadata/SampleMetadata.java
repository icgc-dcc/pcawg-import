package org.icgc.dcc.pcawg.client.data.metadata;

public interface SampleMetadata {

  String getAliquotId();

  org.icgc.dcc.pcawg.client.vcf.WorkflowTypes getWorkflowType();

  boolean isUsProject();

  String getAnalyzedSampleId();

  String getDccProjectCode();

  String getMatchedSampleId();

  String getAnalyzedFileId();

  String getMatchedFileId();
}
