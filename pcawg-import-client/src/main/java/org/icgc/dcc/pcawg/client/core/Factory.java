package org.icgc.dcc.pcawg.client.core;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.fscontroller.FsController;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformer;
import org.icgc.dcc.pcawg.client.download.Portal;
import org.icgc.dcc.pcawg.client.download.PortalStorage;
import org.icgc.dcc.pcawg.client.model.ssm.SSMValidator;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadataFieldMapping;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimaryFieldMapping;
import org.icgc.dcc.pcawg.client.tsv.TSVConverter;
import org.icgc.dcc.pcawg.client.tsv.impl.SSMMetadataTSVConverter;
import org.icgc.dcc.pcawg.client.tsv.impl.SSMPrimaryTSVConverter;
import org.icgc.dcc.pcawg.client.utils.DictionaryCreator;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;
import org.icgc.dcc.submission.dictionary.model.FileSchema;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.BARCODE_SHEET_TSV_URL;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.DICTIONARY_CURRENT_URL;
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
import static org.icgc.dcc.pcawg.client.download.PortalStorage.downloadFileByURL;
import static org.icgc.dcc.pcawg.client.download.PortalStorage.newStorage;
import static org.icgc.dcc.pcawg.client.download.query.PortalVcfFileQueryCreator.newPcawgQueryCreator;

@NoArgsConstructor(access = PRIVATE)
@Slf4j
public class Factory {

  private static final TSVConverter<SSMMetadata> SSM_METADATA_TSV_CONVERTER = new SSMMetadataTSVConverter();
  private static final TSVConverter<SSMPrimary> SSM_PRIMARY_TSV_CONVERTER = new SSMPrimaryTSVConverter();
  private static final boolean APPEND_DCC_TRANSFORMERS = false;

  public static PortalStorage newDefaultStorage() {
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


  public static void  downloadSheet(String url, String outputFilename){
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
