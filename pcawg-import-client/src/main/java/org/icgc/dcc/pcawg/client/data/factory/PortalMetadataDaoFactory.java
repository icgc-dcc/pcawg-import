package org.icgc.dcc.pcawg.client.data.factory;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.portal.PortalMetadataDao;
import org.icgc.dcc.pcawg.client.download.Portal;
import org.icgc.dcc.pcawg.client.download.PortalFiles;
import org.icgc.dcc.pcawg.client.download.query.PortalAllVcfFileQueryCreator;
import org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorerFactory;

import java.io.IOException;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.PORTAL_METADATA_DAO_PERSISTANCE_FILENAME;
import static org.icgc.dcc.pcawg.client.data.portal.PortalMetadataDao.newPortalMetadataDao;
import static org.icgc.dcc.pcawg.client.download.query.PortalAllVcfFileQueryCreator.newPcawgQueryCreator;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;

@RequiredArgsConstructor
public class PortalMetadataDaoFactory {

  private static final PortalAllVcfFileQueryCreator CONSENSUS_VCF_PORTAL_QUERY_CREATOR = newPcawgQueryCreator(CONSENSUS);

  public static final PortalMetadataDaoFactory newPortalMetadataDaoFactory(LocalFileRestorerFactory localFileRestorerFactory){
    val portal = Portal.builder().jsonQueryGenerator(CONSENSUS_VCF_PORTAL_QUERY_CREATOR).build();
    return new PortalMetadataDaoFactory(portal, localFileRestorerFactory);
  }

  @NonNull  private final Portal portal;
  @NonNull  private final LocalFileRestorerFactory localFileRestorerFactory;

  private PortalMetadataDao _createPortalMetadataDao(){
    val list = portal.getFileMetas()
        .stream()
        .map(PortalFiles::convertToPortalMetadata)
        .collect(toImmutableList());
    return newPortalMetadataDao(list);
  }

  public PortalMetadataDao createPortalMetadataDao() throws IOException, ClassNotFoundException {
    val restorer = localFileRestorerFactory.<PortalMetadataDao>createFileRestorer(PORTAL_METADATA_DAO_PERSISTANCE_FILENAME);
    if (restorer.isPersisted()){
      return restorer.restore();
    } else {
      val dao = _createPortalMetadataDao();
      restorer.store(dao);
      return dao;
    }
  }

}
