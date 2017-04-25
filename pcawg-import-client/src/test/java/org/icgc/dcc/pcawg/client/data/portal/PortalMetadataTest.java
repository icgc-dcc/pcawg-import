package org.icgc.dcc.pcawg.client.data.portal;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorerFactory;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.data.factory.PortalMetadataDaoFactory.newAllPortalMetadataDaoFactory;
import static org.icgc.dcc.pcawg.client.model.portal.PortalFilename.newPortalFilename;

@Slf4j
public class PortalMetadataTest {

  @Test
  @SneakyThrows
  public void testQuery(){
    val localFileRestorerFactory = LocalFileRestorerFactory.newFileRestorerFactory("test.persisted");
    val factory = newAllPortalMetadataDaoFactory(localFileRestorerFactory);
    val portalMetadataDao = factory.createPortalMetadataDao();

    val portalFilename = newPortalFilename("f8467ec8-2d61-ba21-e040-11ac0c483584.consensus.20161006.somatic.indel.vcf.gz");
    val request = PortalMetadataRequest.newPortalMetadataRequest(portalFilename);
    val resultList = portalMetadataDao.find(request);
    assertThat(resultList).isNotEmpty().isNotNull();
    assertThat(resultList).hasSize(1);

    val portalMetadataList = portalMetadataDao.findAll();
    val actualSize = portalMetadataList.size();
    val expectedSize = portalMetadataList.stream().collect(toImmutableSet()).size();
    assertThat(actualSize).isEqualTo(expectedSize);

  }

  @Test
  public void testPortalFilename(){
    val portalFilename = newPortalFilename("f8467ec8-2d61-ba21-e040-11ac0c483584.consensus.20161006.somatic.indel.vcf.gz");
    val portalFilenameCopy = newPortalFilename("f8467ec8-2d61-ba21-e040-11ac0c483584.consensus.20161006.somatic.indel.vcf.gz");
    assertThat(portalFilename == portalFilenameCopy).isFalse();
    assertThat(portalFilename).isEqualTo(portalFilenameCopy);
  }

}
