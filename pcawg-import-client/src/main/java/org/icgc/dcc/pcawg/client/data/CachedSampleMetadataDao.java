package org.icgc.dcc.pcawg.client.data;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.pcawg.client.model.metadata.file.PortalFilename;
import org.icgc.dcc.pcawg.client.model.metadata.project.SampleMetadata;

import java.util.Map;
import java.util.Set;

public class CachedSampleMetadataDao implements SampleMetadataDAO{

  private final SampleMetadataDAO  internalDao;

  private final Map<String, SampleMetadata> map;

  public static CachedSampleMetadataDao newCachedSampleMetadataDao(SampleMetadataDAO sampleMetadataDAO, Set<PortalFilename> portalFilenames){
    return new CachedSampleMetadataDao(sampleMetadataDAO, portalFilenames);
  }

  private CachedSampleMetadataDao(@NonNull SampleMetadataDAO sampleMetadataDAO, @NonNull Set<PortalFilename> portalFilenames){
    this.internalDao = sampleMetadataDAO;
    this.map = initMap(sampleMetadataDAO, portalFilenames);
  }

  @SneakyThrows
  private static Map<String, SampleMetadata> initMap(SampleMetadataDAO dao, Set<PortalFilename> portalFilenames){
    val outMap = Maps.<String, SampleMetadata>newHashMap();
    for (val pn : portalFilenames){
      val aliquotId = pn.getAliquotId();
      if (!outMap.containsKey(aliquotId)){
        val sampleMetadata = dao.fetchSampleMetadata(pn);
        outMap.put(sampleMetadata.getAliquotId(), sampleMetadata);
      }
    }
    return ImmutableMap.copyOf(outMap);
  }

  @Override
  public SampleMetadata fetchSampleMetadata(@NonNull PortalFilename portalFilename) throws SampleMetadataNotFoundException {
    val aliquotId = portalFilename.getAliquotId();
    if (map.containsKey(aliquotId)){
      return map.get(portalFilename.getAliquotId());
    } else {
      throw new SampleMetadataNotFoundException(
          String.format("Could not find the sampleMetadata for the PortalFilename [%s]", portalFilename));
    }

  }

}
