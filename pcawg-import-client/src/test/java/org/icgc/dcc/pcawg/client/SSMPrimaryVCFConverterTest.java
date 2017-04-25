package org.icgc.dcc.pcawg.client;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.metadata.BasicSampleMetadata;
import org.icgc.dcc.pcawg.client.data.metadata.ConsensusSampleMetadata;
import org.icgc.dcc.pcawg.client.model.portal.PortalFilename;
import org.icgc.dcc.pcawg.client.data.portal.PortalMetadataDao;
import org.icgc.dcc.pcawg.client.filter.coding.SnpEffCodingFilter;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory;
import org.icgc.dcc.pcawg.client.utils.measurement.CounterMonitor;
import org.icgc.dcc.pcawg.client.model.types.WorkflowTypes;
import org.icgc.dcc.pcawg.client.vcf.converters.variant.strategy.VariantConverterStrategyMux;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.pcawg.client.data.factory.PortalMetadataDaoFactory.newCollabPortalMetadataDaoFactory;
import static org.icgc.dcc.pcawg.client.model.portal.PortalFilename.newPortalFilename;
import static org.icgc.dcc.pcawg.client.data.portal.PortalMetadataRequest.newPortalMetadataRequest;
import static org.icgc.dcc.pcawg.client.storage.impl.PortalStorage.newPortalStorage;
import static org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory.newVariantFilterFactory;
import static org.icgc.dcc.pcawg.client.utils.persistance.LocalFileRestorerFactory.newFileRestorerFactory;
import static org.icgc.dcc.pcawg.client.model.types.WorkflowTypes.CONSENSUS;
import static org.icgc.dcc.pcawg.client.vcf.converters.file.MetadataDTCConverter.newMetadataDTCConverter;
import static org.icgc.dcc.pcawg.client.vcf.converters.file.PrimaryDTCConverter.newPrimaryDTCConverter;
import static org.icgc.dcc.pcawg.client.vcf.converters.file.VCFStreamFilter.newVCFStreamFilter;
import static org.icgc.dcc.pcawg.client.vcf.converters.variant.ConsensusVariantProcessor.newConsensusVariantProcessor;

@Slf4j
@Ignore
public class SSMPrimaryVCFConverterTest {

  private static PortalMetadataDao portalMetadataDao;
  private static final Path scratchPath = Paths.get("scratch");
  private static final Path tempDownloadPath = scratchPath.resolve("tempDownload");
  private static final Path persistedPath = scratchPath.resolve("persisted");
  private static VariantFilterFactory variantAllFilterFactory;
  private static SnpEffCodingFilter codingFilter;

  @BeforeClass
  @SneakyThrows
  public static void init(){
    val fileRestorerFactory = newFileRestorerFactory(persistedPath.toString());
    val portalMetadataDaoFactory = newCollabPortalMetadataDaoFactory(fileRestorerFactory);
    portalMetadataDao = portalMetadataDaoFactory.createPortalMetadataDao();
    variantAllFilterFactory = newVariantFilterFactory(false, false);
    codingFilter = variantAllFilterFactory.getSnpEffCodingFilter();
  }

  @SneakyThrows
  private File getVCFFile(PortalFilename portalFilename){
    val token = System.getProperty("token");
    assertThat(token).describedAs("Token is null").isNotNull();
    val storage = newPortalStorage(false, tempDownloadPath.toString(),false, token);
    val result = portalMetadataDao.findFirst(newPortalMetadataRequest(portalFilename));
    assertThat(result.isPresent()).isTrue();
    val portalMetadata = result.get();
    return storage.getFile(portalMetadata);
  }

  @Test
  @SneakyThrows
  public void testStreamUsNoFilters(){
    val filename = "f8515e5a-7de3-6be3-e040-11ac0c480d6d.consensus.20160830.somatic.snv_mnv.vcf.gz";
    val  isUsProject = true;
    val bypassTcgaFilter = true;
    val bypassNoiseFilter = true;
    val totalNumVariants = 2702;
    val filteredNumVariants = 2702;
    runStreamSSMPrimary(filename, isUsProject, bypassTcgaFilter, bypassNoiseFilter, totalNumVariants, filteredNumVariants);
  }

  @Test
  @SneakyThrows
  public void testStreamUsAllFilters(){
    val  isUsProject = true;
    val bypassTcgaFilter = false;
    val bypassNoiseFilter = false;
    val totalNumVariants = 2702;
    val filteredNumVariants = 13;
    runDefaultStreamSSMPrimary(isUsProject, bypassTcgaFilter, bypassNoiseFilter, totalNumVariants, filteredNumVariants);
  }

  @Test
  @SneakyThrows
  public void testStreamUsTcgaFilter(){
    val  isUsProject = true;
    val bypassTcgaFilter = false;
    val bypassNoiseFilter = true;
    val totalNumVariants = 2702;
    val filteredNumVariants = 24;
    runDefaultStreamSSMPrimary(isUsProject, bypassTcgaFilter, bypassNoiseFilter, totalNumVariants, filteredNumVariants);
  }

  @Test
  @SneakyThrows
  public void testStreamUsNoiseFilter(){
    val  isUsProject = true;
    val bypassTcgaFilter = true;
    val bypassNoiseFilter = false;
    val totalNumVariants = 2702;
    val filteredNumVariants = 1201;
    runDefaultStreamSSMPrimary(isUsProject, bypassTcgaFilter, bypassNoiseFilter, totalNumVariants, filteredNumVariants);
  }

  @Test
  @SneakyThrows
  public void testStreamNonUsNoFilter(){
    val  isUsProject = false;
    val bypassTcgaFiltering = true;
    val bypassNoiseFiltering = true;
    val totalNumVariants = 2702;
    val filteredNumVariants = 2702;
    runDefaultStreamSSMPrimary(isUsProject, bypassTcgaFiltering, bypassNoiseFiltering, totalNumVariants, filteredNumVariants);
  }
  @Test
  @SneakyThrows
  public void testStreamNonUsTcgaFilter(){
    val  isUsProject = false;
    val bypassTcgaFiltering = false;
    val bypassNoiseFiltering = true;
    val totalNumVariants = 2702;
    val filteredNumVariants = 2702;
    runDefaultStreamSSMPrimary(isUsProject, bypassTcgaFiltering, bypassNoiseFiltering, totalNumVariants, filteredNumVariants);
  }

  @Test
  @SneakyThrows
  public void testStreamNonUsNoiseFilter(){
    val  isUsProject = false;
    val bypassTcgaFiltering = true;
    val bypassNoiseFiltering = false;
    val totalNumVariants = 2702;
    val filteredNumVariants = 1201;
    runDefaultStreamSSMPrimary(isUsProject, bypassTcgaFiltering, bypassNoiseFiltering, totalNumVariants, filteredNumVariants);
  }
  @Test
  @SneakyThrows
  public void testStreamNonUsAllFilters(){
    val  isUsProject = false;
    val bypassTcgaFiltering = false;
    val bypassNoiseFiltering = false;
    val totalNumVariants = 2702;
    val filteredNumVariants = 1201;
    runDefaultStreamSSMPrimary(isUsProject, bypassTcgaFiltering, bypassNoiseFiltering, totalNumVariants, filteredNumVariants);
  }

  @SneakyThrows
  private void runDefaultStreamSSMPrimary(boolean isUsProject, boolean bypassTcgaFiltering, boolean bypassNoiseFiltering, int totalNumVariants, int filteredNumVariants){
    val filename = "f8515e5a-7de3-6be3-e040-11ac0c480d6d.consensus.20160830.somatic.snv_mnv.vcf.gz";
    runStreamSSMPrimary(filename, isUsProject, bypassTcgaFiltering, bypassNoiseFiltering, totalNumVariants, filteredNumVariants);
  }

  @SneakyThrows
  private void runStreamSSMPrimary(String filename, boolean isUsProject, boolean bypassTcgaFiltering, boolean bypassNoiseFiltering, int totalNumVariants, int filteredNumVariants){
    val portalFilename = newPortalFilename(filename);
    val workflowType = WorkflowTypes.parseMatch(portalFilename.getWorkflow(), true);
    assertThat(workflowType).isEqualTo(CONSENSUS);

    val file = getVCFFile(portalFilename);
    val vcfPath = file.toPath();
    val sampleMetadataConsensus = BasicSampleMetadata.builder()
        .analyzedSampleId("a")
        .matchedSampleId("b")
        .aliquotId("q1w2e3r4t5")
        .analyzedFileId("F1a")
        .dccProjectCode("ROB-UK")
        .isUsProject(isUsProject)
        .matchedFileId("F1b")
        .workflowType(CONSENSUS)
        .build();

    val variantFilterFactory = newVariantFilterFactory(codingFilter, bypassTcgaFiltering, bypassNoiseFiltering);
    val vcfStreamFilter = newVCFStreamFilter(vcfPath,sampleMetadataConsensus,variantFilterFactory);
    vcfStreamFilter.streamFilteredVariants();

    assertThat(vcfStreamFilter.getTotalNumVariants()).isEqualTo(totalNumVariants);
    assertThat(vcfStreamFilter.getFilteredNumVariants()).isEqualTo(filteredNumVariants);
  }

  private static final String DUMMY_ALIQUOT_ID = "1q2w3e4r5t";
  private static final String DUMMY_DCC_PROJECT_CODE = "BOCA-UK";
  private static final String DUMMY_MATCHED_SAMPLE_ID = "PD03a";
  private static final String DUMMY_ANALYZED_SAMPLE_ID = "PD03b";
  private static final String DUMMY_MATCHED_FILE_ID = "F03a";
  private static final String DUMMY_ANALYZED_FILE_ID = "F03b";


  private static ConsensusSampleMetadata createConsensusSampleMetadata(boolean isUsProject, WorkflowTypes workflowType){
    return ConsensusSampleMetadata.builder()
        .aliquotId(DUMMY_ALIQUOT_ID)
        .dccProjectCode(DUMMY_DCC_PROJECT_CODE)
        .isUsProject(isUsProject)
        .matchedSampleId(DUMMY_MATCHED_SAMPLE_ID)
        .matchedFileId(DUMMY_MATCHED_FILE_ID)
        .analyzedSampleId(DUMMY_ANALYZED_SAMPLE_ID)
        .analyzedFileId(DUMMY_ANALYZED_FILE_ID)
        .build();
  }
  @Test
  public void testConvert(){
    val isUsProject = true;
    val workflowType = CONSENSUS;
    val bypassTcgaFiltering = true;
    val bypassNoiseFiltering = true;
    val filename = "f8515e5a-7de3-6be3-e040-11ac0c480d6d.consensus.20160830.somatic.snv_mnv.vcf.gz";
    val portalFilename = newPortalFilename(filename);
    val file = getVCFFile(portalFilename);
    val vcfPath = file.toPath();
    val variantFilterFactory = newVariantFilterFactory(codingFilter, bypassTcgaFiltering, bypassNoiseFiltering);
    val consensusSampleMetadata = createConsensusSampleMetadata(isUsProject, workflowType);
    val variantConverterStrategyMux = new VariantConverterStrategyMux();
    val consensusVariantProcessor = newConsensusVariantProcessor(consensusSampleMetadata, variantConverterStrategyMux);
    val metadataDTCConverter  = newMetadataDTCConverter();
    val primaryDTCConverter = newPrimaryDTCConverter(consensusVariantProcessor);
    val primaryCounterMonitor = CounterMonitor.newMonitor("testPRIMARY", 100000);



  }


}
