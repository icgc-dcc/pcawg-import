package org.icgc.dcc.pcawg.client.download;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.SampleMetadataDAO;
import org.icgc.dcc.pcawg.client.model.metadata.MetadataContext;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.groupingBy;
import static org.icgc.dcc.pcawg.client.model.metadata.file.PortalMetadata.buildPortalMetadata;

@Getter
@Slf4j
public class MetadataContainer {

  private List<MetadataContext> metadataContextList;

  private Map<String, List<MetadataContext>> dccProjectCodeMap;

  public MetadataContainer(@NonNull Portal portal, @NonNull SampleMetadataDAO sampleMetadataDAO){
    init(portal, sampleMetadataDAO);
  }

  private void init(Portal portal, SampleMetadataDAO sampleMetadataDAO){
    val builder = ImmutableList.<MetadataContext>builder();
    for (val fileMeta : portal.getFileMetas()){
      val portalMetadata = buildPortalMetadata(fileMeta);
      val filenameParser = portalMetadata.getPortalFilename();
      try{
        val sampleMetadata = sampleMetadataDAO.fetchSampleMetadata(filenameParser);

        builder.add(MetadataContext.builder()
            .sampleMetadata(sampleMetadata)
            .portalMetadata(portalMetadata)
            .build());
      } catch (SampleMetadataDAO.SampleMetadataNotFoundException e){
        log.error("The sampleMetadata cannot be fetched for the file [{}]. Skipping.. ", filenameParser.getFilename());
      }
    }
    metadataContextList = builder.build();
    dccProjectCodeMap = groupByDccProjectCode(metadataContextList);
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
