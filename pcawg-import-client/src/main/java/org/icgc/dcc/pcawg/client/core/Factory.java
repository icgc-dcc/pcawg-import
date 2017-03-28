package org.icgc.dcc.pcawg.client.core;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.FileSampleMetadataBeanDAO;
import org.icgc.dcc.pcawg.client.data.FileSampleMetadataFastDAO;
import org.icgc.dcc.pcawg.client.data.SampleMetadataDAO;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBeanDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleBeanDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleBeanDaoOld;
import org.icgc.dcc.pcawg.client.download.MetadataContainer;
import org.icgc.dcc.pcawg.client.download.Portal;
import org.icgc.dcc.pcawg.client.download.PortalFileDownloader;
import org.icgc.dcc.pcawg.client.download.Storage;
import org.icgc.dcc.pcawg.client.model.metadata.project.SampleMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.impl.PcawgSSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.tsv.TSVConverter;
import org.icgc.dcc.pcawg.client.tsv.impl.SSMMetadataTSVConverter;
import org.icgc.dcc.pcawg.client.tsv.impl.SSMPrimaryTSVConverter;
import org.icgc.dcc.pcawg.client.vcf.VariationCallingAlgorithms;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkState;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SAMPLE_SHEET_HAS_HEADER;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SAMPLE_SHEET_TSV_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SAMPLE_SHEET_TSV_URL;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.STORAGE_BYPASS_MD5_CHECK;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.STORAGE_PERSIST_MODE;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.TOKEN;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.UUID2BARCODE_SHEET_HAS_HEADER;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.UUID2BARCODE_SHEET_TSV_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.UUID2BARCODE_SHEET_TSV_URL;
import static org.icgc.dcc.pcawg.client.data.FileSampleMetadataFastDAO.newFileSampleMetadataFastDAO;
import static org.icgc.dcc.pcawg.client.download.PortalQueryCreator.newPcawgQueryCreator;
import static org.icgc.dcc.pcawg.client.download.Storage.downloadFileByURL;
import static org.icgc.dcc.pcawg.client.download.Storage.newStorage;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;

@NoArgsConstructor(access = PRIVATE)
@Slf4j
public class Factory {

  private static final TSVConverter<SSMMetadata> SSM_METADATA_TSV_CONVERTER = new SSMMetadataTSVConverter();
  private static final TSVConverter<SSMPrimary> SSM_PRIMARY_TSV_CONVERTER = new SSMPrimaryTSVConverter();

  public static TransformerFactory<SSMMetadata> newSSMMetadataTransformerFactory(boolean useHdfs){
    return TransformerFactory.newTransformerFactory(SSM_METADATA_TSV_CONVERTER, useHdfs);
  }

  public static TransformerFactory<SSMPrimary> newSSMPrimaryTransformerFactory(boolean useHdfs){
    return TransformerFactory.newTransformerFactory(SSM_PRIMARY_TSV_CONVERTER, useHdfs);
  }

  public static Storage newDefaultStorage() {
    log.info("Creating storage instance with persistMode: {}, outputDir: {}, and md5BypassEnable: {}",
        STORAGE_PERSIST_MODE, STORAGE_OUTPUT_VCF_STORAGE_DIR, STORAGE_BYPASS_MD5_CHECK, TOKEN);
    return newStorage(STORAGE_PERSIST_MODE, STORAGE_OUTPUT_VCF_STORAGE_DIR, STORAGE_BYPASS_MD5_CHECK, TOKEN);
  }


  public static Portal newPortal(WorkflowTypes callerType){
    log.info("Creating new Portal instance for callerType [{}]", callerType.name());
    return Portal.builder()
        .jsonQueryGenerator(newPcawgQueryCreator(callerType))
        .build();
  }

  public static MetadataContainer newMetadataContainer(){
    val portal = newPortal(CONSENSUS);
    val sampleMetadataDAO = newSampleMetadataDAO();
    log.info("Creating MetadataContainer");
    return new MetadataContainer(portal, sampleMetadataDAO);
  }

  private static PortalFileDownloader newPortalFileDownloader(WorkflowTypes callerType){
    return PortalFileDownloader.newPortalFileDownloader(newPortal(callerType), newDefaultStorage());
  }

  public static PortalFileDownloader newConsensusPortalFileDownloader(){
    return newPortalFileDownloader(CONSENSUS);
  }

  public static SSMMetadata newSSMMetadata(SampleMetadata sampleMetadata){
    val workflowType = sampleMetadata.getWorkflowType();
    val dataType = sampleMetadata.getDataType();
    return PcawgSSMMetadata.newSSMMetadataImpl(
        VariationCallingAlgorithms.get(workflowType, dataType),
        dataType.getName(),
        sampleMetadata.getMatchedSampleId(),
        sampleMetadata.getAnalysisId(),
        sampleMetadata.getAnalyzedSampleId(),
        sampleMetadata.isUsProject(),
        sampleMetadata.getAliquotId() );
  }

  private static void  downloadSheet(String url, String outputFilename){
    val outputDir = Paths.get("").toAbsolutePath().toString();
    if (Paths.get(outputFilename).toFile().exists()){
      log.info("Already downloaded [{}] to directory [{}] from url: {}", outputFilename, outputDir,url);
    } else {
      log.info("Downloading [{}] to directory [{}] from url: {}", outputFilename, outputDir,url);
      downloadFileByURL(url, outputFilename);
    }
  }

  public static void downloadSampleSheet(String outputFilename){
    downloadSheet(SAMPLE_SHEET_TSV_URL, outputFilename);
  }

  public static void downloadUUID2BarcodeSheet(String outputFilename){
    downloadSheet(UUID2BARCODE_SHEET_TSV_URL, outputFilename);
  }

  private static SampleBeanDaoOld initSampleDao(){
    val outputFilename = SAMPLE_SHEET_TSV_FILENAME;
    val url = SAMPLE_SHEET_TSV_URL;
    val outputDir = Paths.get("").toAbsolutePath().toString();

    checkState(outputFilename.endsWith(".tsv"),
        "The output filename %s does not end with \".tsv\"", outputFilename);
    if (Paths.get(outputFilename).toFile().exists()){
      log.info("Already downloaded [{}] to directory [{}] from url: {}", outputFilename, outputDir,url);
      outputFilename.replace(".tsv",".dat");
    } else {
      log.info("Downloading [{}] to directory [{}] from url: {}", outputFilename, outputDir,url);
      downloadFileByURL(url, outputFilename);
    }
    return null;
  }


  @SneakyThrows
  public static FileSampleMetadataBeanDAO newFileSampleMetadataBeanDAOAndDownload(){
    downloadSampleSheet(SAMPLE_SHEET_TSV_FILENAME);
    downloadUUID2BarcodeSheet(UUID2BARCODE_SHEET_TSV_FILENAME);
    val sampleDao = SampleBeanDao.newSampleBeanDao(SAMPLE_SHEET_TSV_FILENAME);
    val barcodeDao = BarcodeBeanDao.newBarcodeBeanDao(UUID2BARCODE_SHEET_TSV_FILENAME);
    log.info("Done downloading, creating FileSampleMetadataFastDAO");
    return new FileSampleMetadataBeanDAO(sampleDao, barcodeDao);
  }

  @SneakyThrows
  public static FileSampleMetadataFastDAO newFileSampleMetadataFastDAOAndDownload(){
    downloadSampleSheet(SAMPLE_SHEET_TSV_FILENAME);
    downloadUUID2BarcodeSheet(UUID2BARCODE_SHEET_TSV_FILENAME);
    log.info("Done downloading, creating FileSampleMetadataFastDAO");
    return newFileSampleMetadataFastDAO(SAMPLE_SHEET_TSV_FILENAME,
        SAMPLE_SHEET_HAS_HEADER, UUID2BARCODE_SHEET_TSV_FILENAME, UUID2BARCODE_SHEET_HAS_HEADER);
  }


  public static SampleMetadataDAO newSampleMetadataDAO(){
    return newFileSampleMetadataBeanDAOAndDownload(); //Uses beans, but slow loading
//    return newFileSampleMetadataFastDAOAndDownload(); // USes custom parser
  }

}
