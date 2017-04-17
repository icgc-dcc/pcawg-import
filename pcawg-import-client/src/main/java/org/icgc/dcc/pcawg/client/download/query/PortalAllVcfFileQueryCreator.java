package org.icgc.dcc.pcawg.client.download.query;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.icgc.dcc.pcawg.client.core.ObjectNodeConverter;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;
import static org.icgc.dcc.pcawg.client.utils.Strings.toStringArray;

@RequiredArgsConstructor(access = PRIVATE)
@Value
@Slf4j
public class PortalAllVcfFileQueryCreator implements ObjectNodeConverter {

  public static final PortalAllVcfFileQueryCreator newPortalAllVcfFileQueryCreator(WorkflowTypes callerType){
    log.info("Creating {} instance for callertype [{}]",PortalAllVcfFileQueryCreator.class.getSimpleName(), callerType.name());
    return new PortalAllVcfFileQueryCreator(callerType);
  }

  @NonNull
  private final WorkflowTypes workflowType;

  @Override
  public ObjectNode toObjectNode(){
    return object()
        .with("file",
            object()
                .with("dataType", createIs("SSM"))
                .with("study", createIs("PCAWG"))
                .with("experimentalStrategy", createIs("WGS"))
                .with("fileFormat", createIs("VCF"))
                .with("software", createIs(toStringArray(workflowType.getPortalSoftwareNames())))
            )
        .end();
  }

}
