package org.icgc.dcc.pcawg.client.data.portal;

import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.icgc.dcc.pcawg.client.core.model.portal.PortalMetadata;
import org.icgc.dcc.pcawg.client.data.BasicDao;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

@RequiredArgsConstructor
public class PortalMetadataDao implements BasicDao<PortalMetadata, PortalMetadataRequest>, Serializable {

  public static final long serialVersionUID = 1492088726L;

  public static final PortalMetadataDao newPortalMetadataDao(List<PortalMetadata> data){
    return new PortalMetadataDao(data);
  }


  @NonNull private final List<PortalMetadata> data;

  private Stream<PortalMetadata> getStream(PortalMetadataRequest request){
    return data.stream()
        .filter(x -> x.getPortalFilename().equals(request.getPortalFilename()));
  }

  @Override public List<PortalMetadata> find(PortalMetadataRequest request) {
    return getStream(request).collect(toImmutableList());
  }

  @Override public List<PortalMetadata> findAll() {
    return ImmutableList.copyOf(data);
  }

  public Optional<PortalMetadata> findFirst(PortalMetadataRequest request){
    return getStream(request).findFirst();
  }

}
