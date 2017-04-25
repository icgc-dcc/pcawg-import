package org.icgc.dcc.pcawg.client.download;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.model.portal.PortalMetadata;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadataDAO;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadataNotFoundException;
import org.icgc.dcc.pcawg.client.download.context.MetadataContext;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.groupingBy;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;

@Getter
@Slf4j
public class MetadataContainer implements Serializable {

  public static final long serialVersionUID = 1491936567L;

  private List<MetadataContext> metadataContextList;

  private Map<String, List<MetadataContext>> dccProjectCodeMap;

  public MetadataContainer(@NonNull SampleMetadataDAO sampleMetadataDAO, @NonNull Set<PortalMetadata> portalMetadatas){
    init(sampleMetadataDAO, portalMetadatas);
  }

  private void init(SampleMetadataDAO sampleMetadataDAO, Set<PortalMetadata> portalMetadatas){
    val builder = ImmutableList.<MetadataContext>builder();
    for (val portalMetadata : portalMetadatas){
      val portalFilename = portalMetadata.getPortalFilename();
      try{
        val sampleMetadata = sampleMetadataDAO.fetchSampleMetadata(portalFilename);
        builder.add(MetadataContext.builder()
            .sampleMetadata(sampleMetadata)
            .portalMetadata(portalMetadata)
            .build());
      } catch (SampleMetadataNotFoundException e){
        log.error("The sampleMetadata cannot be fetched for the file [{}]. Skipping.. ", portalFilename.getFilename());
      }
    }
    metadataContextList = builder.build();
    dccProjectCodeMap = groupByDccProjectCode(metadataContextList);
  }

  private void init(Portal portal, SampleMetadataDAO sampleMetadataDAO){
    val set = portal.getFileMetas().stream()
        .map(PortalFiles::convertToPortalMetadata)
        .collect(toImmutableSet());
    init(sampleMetadataDAO, set);
  }

  public int getTotalMetadataContexts(){
    return getMetadataContexts().size();
  }

  //Lazy loading
  public List<MetadataContext> getMetadataContexts(){
    return metadataContextList;
  }

  public Set<String> getDccProjectCodes(){
    return dccProjectCodeMap.keySet();
  }

  public List<MetadataContext> getMetadataContexts(String dccProjectCode){
    checkArgument(dccProjectCodeMap.containsKey(dccProjectCode), "The dccProjectCode [%s] does not exist", dccProjectCode);
    return dccProjectCodeMap.get(dccProjectCode);
  }

  private static Map<String, List<MetadataContext>> groupByDccProjectCode(List<MetadataContext> metadataContexts){
    return metadataContexts
        .stream()
        .collect(groupingBy(x -> x.getSampleMetadata().getDccProjectCode()));
  }

}
