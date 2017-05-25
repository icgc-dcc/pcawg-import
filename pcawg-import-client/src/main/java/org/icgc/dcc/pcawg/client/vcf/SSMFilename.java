package org.icgc.dcc.pcawg.client.vcf;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.common.core.util.Joiners.DOT;

@RequiredArgsConstructor
public enum SSMFilename {

  SSM_P("ssm_p", "[a-zA-Z0-9]+", "txt", "0.16a" ),
  SSM_M("ssm_m", "[a-zA-Z0-9]+", "txt", "0.16a" );

  @NonNull private final String prefix;
  @NonNull private final String middleRegexRule;
  @NonNull private final String suffix;
  @NonNull private final String dictionaryViewerVersion;

  public String getFilename(WorkflowTypes workflowType){
    val workflowTypeName = workflowType.getName();

    //This check is neccessary for the ETL process, which follows the dictionary filenaming conventions
    checkArgument(workflowTypeName.matches(middleRegexRule),
        "The workflowType [%s] does not match the specified regex [%s] for dictionaryViewerVersion=%s",
        workflowTypeName, middleRegexRule, dictionaryViewerVersion);
    return DOT.join(prefix, workflowTypeName, suffix );
  }

  public String getFilename(){
    return DOT.join(prefix, suffix );
  }

}
