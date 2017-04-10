package org.icgc.dcc.pcawg.client.core;

import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.fscontroller.FsController;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformer;
import org.icgc.dcc.pcawg.client.data.barcode.impl.BarcodeSheetFastDao;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadataDAO;
import org.icgc.dcc.pcawg.client.data.metadata.impl.FileSampleMetadataBeanDAO;
import org.icgc.dcc.pcawg.client.data.sample.impl.SampleSheetFastDao;
import org.icgc.dcc.pcawg.client.download.MetadataContainer;
import org.icgc.dcc.pcawg.client.download.Portal;
import org.icgc.dcc.pcawg.client.download.Storage;
import org.icgc.dcc.pcawg.client.model.portal.PortalMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.SSMValidator;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadataFieldMapping;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.impl.PcawgSSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimaryFieldMapping;
import org.icgc.dcc.pcawg.client.tsv.TSVConverter;
import org.icgc.dcc.pcawg.client.tsv.impl.SSMMetadataTSVConverter;
import org.icgc.dcc.pcawg.client.tsv.impl.SSMPrimaryTSVConverter;
import org.icgc.dcc.pcawg.client.utils.DictionaryCreator;
import org.icgc.dcc.pcawg.client.vcf.VariationCallingAlgorithms;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;
import org.icgc.dcc.submission.dictionary.model.FileSchema;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.BARCODE_BEAN_DAO_PERSISTANCE_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.BARCODE_SHEET_HAS_HEADER;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.BARCODE_SHEET_TSV_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.BARCODE_SHEET_TSV_URL;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.DICTIONARY_CURRENT_URL;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.ICGC_FILE_ID_DAO_PERSISTANCE_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SAMPLE_BEAN_DAO_PERSISTANCE_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SAMPLE_SHEET_HAS_HEADER;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SAMPLE_SHEET_TSV_FILENAME;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SAMPLE_SHEET_TSV_URL;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SSM_M_TSV_FILENAME_EXTENSION;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SSM_M_TSV_FILENAME_PREFIX;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SSM_P_TSV_FILENAME_EXTENSION;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.SSM_P_TSV_FILENAME_PREFIX;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.STORAGE_BYPASS_MD5_CHECK;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.STORAGE_PERSIST_MODE;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.TOKEN;
import static org.icgc.dcc.pcawg.client.core.fscontroller.impl.HadoopFsController.newHadoopFsController;
import static org.icgc.dcc.pcawg.client.core.fscontroller.impl.HadoopFsControllerAdapter.newHadoopFsControllerAdapter;
import static org.icgc.dcc.pcawg.client.core.fscontroller.impl.LocalFsController.newLocalFsController;
import static org.icgc.dcc.pcawg.client.data.factory.impl.BarcodeBeanDaoFactory.buildBarcodeSheetBeanDao;
import static org.icgc.dcc.pcawg.client.data.factory.impl.SampleBeanDaoFactory.buildSampleSheetBeanDao;
import static org.icgc.dcc.pcawg.client.data.icgc.FileIdDao.newFileIdDao;
import static org.icgc.dcc.pcawg.client.download.Storage.downloadFileByURL;
import static org.icgc.dcc.pcawg.client.download.Storage.newStorage;
import static org.icgc.dcc.pcawg.client.download.query.PortalVcfFileQueryCreator.newPcawgQueryCreator;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;

@NoArgsConstructor(access = PRIVATE)
@Slf4j
public class Factory {

  private static final TSVConverter<SSMMetadata> SSM_METADATA_TSV_CONVERTER = new SSMMetadataTSVConverter();
  private static final TSVConverter<SSMPrimary> SSM_PRIMARY_TSV_CONVERTER = new SSMPrimaryTSVConverter();
  private static final boolean APPEND_DCC_TRANSFORMERS = false;

  public static Storage newDefaultStorage() {
    log.info("Creating storage instance with persistMode: {}, outputDir: {}, and md5BypassEnable: {}",
        STORAGE_PERSIST_MODE, STORAGE_OUTPUT_VCF_STORAGE_DIR, STORAGE_BYPASS_MD5_CHECK, TOKEN);
    return newStorage(STORAGE_PERSIST_MODE, STORAGE_OUTPUT_VCF_STORAGE_DIR, STORAGE_BYPASS_MD5_CHECK, TOKEN);
  }

  public static DccTransformer<SSMMetadata> newDccMetadataTransformer(FsController<Path> fsController, String outputTsvDir, String dccProjectCode  ){
    val outputDirPath = Paths.get(outputTsvDir);
    return DccTransformer.<SSMMetadata>newDccTransformer(fsController, SSM_METADATA_TSV_CONVERTER,
        outputDirPath,dccProjectCode, SSM_M_TSV_FILENAME_PREFIX,
        SSM_M_TSV_FILENAME_EXTENSION, APPEND_DCC_TRANSFORMERS);
  }

  public static DccTransformer<SSMPrimary> newDccPrimaryTransformer(FsController<Path> fsController, String outputTsvDir, String dccProjectCode  ){
    val outputDirPath = Paths.get(outputTsvDir);
    return DccTransformer.<SSMPrimary>newDccTransformer(fsController, SSM_PRIMARY_TSV_CONVERTER,
        outputDirPath,dccProjectCode, SSM_P_TSV_FILENAME_PREFIX,
        SSM_P_TSV_FILENAME_EXTENSION, APPEND_DCC_TRANSFORMERS);
  }

  public static FsController<Path> newFsController(final boolean isHdfs, Optional<String> optionalHostname, Optional<String> optionalPort){
    if (isHdfs){
      checkArgument(optionalHostname.isPresent());
      checkArgument(optionalPort.isPresent());
      return newHadoopFsControllerAdapter(newHadoopFsController(optionalHostname.get(), optionalPort.get()));
    } else {
      return newLocalFsController();
    }
  }

  public static Portal newPortal(WorkflowTypes callerType){
    log.info("Creating new Portal instance for callerType [{}]", callerType.name());
    return Portal.builder()
        .jsonQueryGenerator(newPcawgQueryCreator(callerType))
        .build();
  }

  public static MetadataContainer newMetadataContainer(){
    log.info("Creating Portal instance for [{}]", CONSENSUS);
    val portal = newPortal(CONSENSUS);

    log.info("Retreiving portal filemeta data...");
    val portalMetadataSet = portal.getFileMetas()
        .stream()
        .map(PortalMetadata::buildPortalMetadata)
        .collect(toImmutableSet());

    log.info("Creating base sampleMetadata DAO...");
    val sampleMetadataDao = newSampleMetadataDAO();

    log.info("Instantiating MetadataContainer");
    return new MetadataContainer(sampleMetadataDao, portalMetadataSet);
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
        sampleMetadata.getAliquotId(),
        sampleMetadata.getAnalyzedFileId(),
        sampleMetadata.getMatchedFileId());
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
    downloadSheet(BARCODE_SHEET_TSV_URL, outputFilename);
  }

  @SneakyThrows
  public static FileSampleMetadataBeanDAO newFileSampleMetadataBeanDAOAndDownload(){
    val sampleDao = buildSampleSheetBeanDao(SAMPLE_SHEET_TSV_URL,SAMPLE_SHEET_TSV_FILENAME,SAMPLE_BEAN_DAO_PERSISTANCE_FILENAME );
    val barcodeDao = buildBarcodeSheetBeanDao(BARCODE_SHEET_TSV_URL,BARCODE_SHEET_TSV_FILENAME,BARCODE_BEAN_DAO_PERSISTANCE_FILENAME );
    val icgcfileIdDao =  newFileIdDao(ICGC_FILE_ID_DAO_PERSISTANCE_FILENAME, sampleDao, barcodeDao);
    log.info("Done initialized SampleFastDao, BarcodeFastDao, and IcgcFileIdDao, ... creating FileSampleMetadataBeanDAO");
    return new FileSampleMetadataBeanDAO(sampleDao, barcodeDao, icgcfileIdDao);
  }

  @SneakyThrows
  public static FileSampleMetadataBeanDAO newFastFileSampleMetadataBeanDAOAndDownload(){
    downloadSampleSheet(SAMPLE_SHEET_TSV_FILENAME);
    downloadUUID2BarcodeSheet(BARCODE_SHEET_TSV_FILENAME);
    val sampleDao = SampleSheetFastDao.newSampleSheetFastDao(SAMPLE_SHEET_TSV_FILENAME, SAMPLE_SHEET_HAS_HEADER);
    val barcodeDao = BarcodeSheetFastDao.newBarcodeSheetFastDao(BARCODE_SHEET_TSV_FILENAME, BARCODE_SHEET_HAS_HEADER);
    val icgcfileIdDao =  newFileIdDao(ICGC_FILE_ID_DAO_PERSISTANCE_FILENAME, sampleDao, barcodeDao);
    log.info("Done initializing SampleFastDao, BarcodeFastDao, and IcgcFileIdDao, ... creating FileSampleMetadataBeanDAO");
    return new FileSampleMetadataBeanDAO(sampleDao, barcodeDao, icgcfileIdDao);
  }

  public static SampleMetadataDAO newSampleMetadataDAO(){
//    return newFastFileSampleMetadataBeanDAOAndDownload(); // Fastest loader, and uses beans, but hardcoded column headers
    return newFileSampleMetadataBeanDAOAndDownload(); //Uses beans, but slow loading
  }

  public static DictionaryCreator buildDictionaryCreator(){
//    return DictionaryCreator.newDictionaryCreator(DICTIONARY_BASE_URL, DICTIONARY_VERSION);
    return DictionaryCreator.newDictionaryCreator(DICTIONARY_CURRENT_URL);
  }

  public static SSMValidator<SSMPrimary, SSMPrimaryFieldMapping> newSSMPrimaryValidator(FileSchema ssmPrimaryFileSchema){
    return SSMValidator.newSSMValidator(ssmPrimaryFileSchema, SSMPrimaryFieldMapping.values());
  }

  public static SSMValidator<SSMMetadata, SSMMetadataFieldMapping> newSSMMetadataValidator(FileSchema ssmMetadataFileSchema){
    return SSMValidator.newSSMValidator(ssmMetadataFileSchema, SSMMetadataFieldMapping.values());
  }

}
