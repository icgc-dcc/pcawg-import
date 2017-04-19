package org.icgc.dcc.pcawg.client;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.data.portal.PortalMetadataDao;
import org.icgc.dcc.pcawg.client.vcf.ConsensusSSMPrimaryConverter;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.pcawg.client.data.factory.PortalMetadataDaoFactory.newCollabPortalMetadataDaoFactory;
import static org.icgc.dcc.pcawg.client.data.portal.PortalFilename.newPortalFilename;
import static org.icgc.dcc.pcawg.client.data.portal.PortalMetadataRequest.newPortalMetadataRequest;
import static org.icgc.dcc.pcawg.client.download.PortalStorage.newPortalStorage;
import static org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory.newVariantFilterFactory;
import static org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorerFactory.newFileRestorerFactory;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;

@Slf4j
public class ConsensusSSMConverterTest {

  private static PortalMetadataDao portalMetadataDao;
  private static final Path scratchPath = Paths.get("scratch");
  private static final Path tempDownloadPath = scratchPath.resolve("tempDownload");
  private static final Path persistedPath = scratchPath.resolve("persisted");

  @BeforeClass
  @SneakyThrows
  public static void init(){
    val fileRestorerFactory = newFileRestorerFactory(persistedPath.toString());
    val portalMetadataDaoFactory = newCollabPortalMetadataDaoFactory(fileRestorerFactory);
    portalMetadataDao = portalMetadataDaoFactory.createPortalMetadataDao();
  }

  @SneakyThrows
  private File getVCFFile(String filename){
    val token = System.getProperty("token","");
    val portalFilename = newPortalFilename(filename);
    val storage = newPortalStorage(false, tempDownloadPath.toString(),false, token);
    val result = portalMetadataDao.findFirst(newPortalMetadataRequest(portalFilename));
    assertThat(result.isPresent()).isTrue();
    val portalMetadata = result.get();
    return storage.getFile(portalMetadata);
  }

  @Test
  @SneakyThrows
  public void testStreamSSMPrimary(){
    String filename = "f8515e5a-7de3-6be3-e040-11ac0c480d6d.consensus.20160830.somatic.snv_mnv.vcf.gz";
    val file = getVCFFile(filename);
    val vcfPath = file.toPath();
    val isUdProject = false;
    val sampleMetadataConsensus = SampleMetadata.builder()
        .analyzedSampleId("a")
        .matchedSampleId("b")
        .aliquotId("q1w2e3r4t5")
        .analyzedFileId("F1a")
        .dccProjectCode("ROB-UK")
        .isUsProject(isUdProject)
        .matchedFileId("F1b")
        .workflowType(CONSENSUS)
        .build();

    val variantFilterFactory = newVariantFilterFactory(true, true);

    val conv = ConsensusSSMPrimaryConverter.newConsensusSSMPrimaryConverter(vcfPath, sampleMetadataConsensus, variantFilterFactory);
    val list = conv.streamSSMPrimary().map(DccTransformerContext::getObject).collect(toList());
    conv.checkForErrors();
    assertThat(conv.getTotalNumVariants()).isEqualTo(2702);
    assertThat(conv.getFilteredNumVariants()).isEqualTo(2702);
    log.info("Done");
  }

}
