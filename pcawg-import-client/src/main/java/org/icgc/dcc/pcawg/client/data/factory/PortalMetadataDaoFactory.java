package org.icgc.dcc.pcawg.client.data.factory;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.portal.PortalMetadataDao;
import org.icgc.dcc.pcawg.client.download.Portal;
import org.icgc.dcc.pcawg.client.download.PortalFiles;
import org.icgc.dcc.pcawg.client.download.query.PortalAllVcfFileQueryCreator;
import org.icgc.dcc.pcawg.client.download.query.PortalCollabVcfFileQueryCreator;
import org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorerFactory;

import java.io.IOException;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.PORTAL_METADATA_DAO_COLLAB_PERSISTANCE_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.PORTAL_METADATA_DAO_NO_COLLAB_PERSISTANCE_FILENAME;
import static org.icgc.dcc.pcawg.client.data.portal.PortalMetadataDao.newPortalMetadataDao;
import static org.icgc.dcc.pcawg.client.download.query.PortalAllVcfFileQueryCreator.newPortalAllVcfFileQueryCreator;
import static org.icgc.dcc.pcawg.client.download.query.PortalCollabVcfFileQueryCreator.newPcawgCollabQueryCreator;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;

@AllArgsConstructor
public class PortalMetadataDaoFactory {

  private static final PortalAllVcfFileQueryCreator CONSENSUS_ALL_VCF_PORTAL_QUERY_CREATOR = newPortalAllVcfFileQueryCreator(CONSENSUS);
  private static final PortalCollabVcfFileQueryCreator CONSENSUS_COLLAB_VCF_PORTAL_QUERY_CREATOR = newPcawgCollabQueryCreator(CONSENSUS);

  public static final PortalMetadataDaoFactory newAllPortalMetadataDaoFactory(LocalFileRestorerFactory localFileRestorerFactory){
    val portal = Portal.builder().jsonQueryGenerator(CONSENSUS_ALL_VCF_PORTAL_QUERY_CREATOR).build();
    return new PortalMetadataDaoFactory(portal, localFileRestorerFactory,
        PORTAL_METADATA_DAO_NO_COLLAB_PERSISTANCE_FILENAME);
  }

  public static final PortalMetadataDaoFactory newCollabPortalMetadataDaoFactory(LocalFileRestorerFactory localFileRestorerFactory){
    val portal = Portal.builder().jsonQueryGenerator(CONSENSUS_COLLAB_VCF_PORTAL_QUERY_CREATOR).build();
    return new PortalMetadataDaoFactory(portal, localFileRestorerFactory,
        PORTAL_METADATA_DAO_COLLAB_PERSISTANCE_FILENAME);
  }

  @NonNull  private final Portal portal;
  @NonNull  private final LocalFileRestorerFactory localFileRestorerFactory;
  @NonNull  private final String portalMetadataDaoPersistanceFilename;

  private PortalMetadataDao _createPortalMetadataDao(){
    val list = portal.getFileMetas()
        .stream()
        .map(PortalFiles::convertToPortalMetadata)
        .collect(toImmutableList());
    return newPortalMetadataDao(list);
  }

  public PortalMetadataDao createPortalMetadataDao() throws IOException, ClassNotFoundException {
    val restorer = localFileRestorerFactory.<PortalMetadataDao>createFileRestorer(portalMetadataDaoPersistanceFilename);
    if (restorer.isPersisted()){
      return restorer.restore();
    } else {
      val dao = _createPortalMetadataDao();
      restorer.store(dao);
      return dao;
    }
  }

}
