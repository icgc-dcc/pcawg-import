package org.icgc.dcc.pcawg.client.core.writer;

import lombok.Builder;
import lombok.NonNull;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static org.icgc.dcc.common.core.util.Joiners.DOT;
import static org.icgc.dcc.common.core.util.Joiners.PATH;

@Builder
public class FileWriterContextFactory {

  @NonNull private final String outputDirectory;
  @NonNull private final String fileNamePrefix;
  @NonNull private final String fileExtension;

  private final boolean append;

  @NonNull
  private final String hostname;

  @NonNull
  private final String port;


  public FileWriterContext getFileWriterContext(WorkflowTypes workflowType, String dccProjectCode){
    return FileWriterContextPojo.builder()
        .append(append)
        .hostname(hostname)
        .outputFilename(createOutputFilename(workflowType, dccProjectCode))
        .port(port)
        .build();
  }

  private String createOutputTsvFilename( WorkflowTypes workflowType){
    return DOT.join(fileNamePrefix, workflowType.getName(), fileExtension);
  }

  private String createOutputFilename(WorkflowTypes workflowType, String dccProjectCode){
    return PATH.join(outputDirectory, dccProjectCode, createOutputTsvFilename(workflowType));
  }

}
