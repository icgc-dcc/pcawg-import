package org.icgc.dcc.pcawg.client;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.DccTransformerFactory;
import org.icgc.dcc.pcawg.client.storage.StorageFactory;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformer;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.data.portal.PortalMetadata;
import org.icgc.dcc.pcawg.client.download.MetadataContainer;
import org.icgc.dcc.pcawg.client.storage.Storage;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory;
import org.icgc.dcc.pcawg.client.model.ssm.SSMValidator;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadataFieldMapping;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimaryFieldMapping;
import org.icgc.dcc.pcawg.client.utils.measurement.CounterMonitor;
import org.icgc.dcc.pcawg.client.utils.measurement.IntegerCounter;
import org.icgc.dcc.pcawg.client.vcf.converters.file.MetadataDTCConverter;
import org.icgc.dcc.pcawg.client.vcf.converters.variant.strategy.VariantConverterStrategyMux;

import java.nio.file.Files;
import java.util.Collection;

import static java.util.stream.Collectors.joining;
import static org.icgc.dcc.common.core.util.Joiners.NEWLINE;
import static org.icgc.dcc.pcawg.client.tsv.TsvValidator.newTsvValidator;
import static org.icgc.dcc.pcawg.client.vcf.converters.file.MetadataDTCConverter.newMetadataDTCConverter;
import static org.icgc.dcc.pcawg.client.vcf.converters.file.PrimaryDTCConverter.newPrimaryDTCConverter;
import static org.icgc.dcc.pcawg.client.vcf.converters.file.VCFStreamFilter.newVCFStreamFilter;
import static org.icgc.dcc.pcawg.client.vcf.converters.variant.ConsensusVariantProcessor.newConsensusVariantProcessor;

@RequiredArgsConstructor
@Slf4j
public class DccProjectProcessor {
  private static final VariantConverterStrategyMux VARIANT_CONVERTER_STRATEGY_MUX = new VariantConverterStrategyMux();
  private static final String FAILED = "**FAILED**";
  private static final String PASSED = "**PASSED**";

  public static DccProjectProcessor newProcessorWithValidation(StorageFactory storageFactory,
      DccTransformerFactory<SSMPrimary> dccPrimaryTransformerFactory,
      DccTransformerFactory<SSMMetadata> dccMetadataTransformerFactory, MetadataContainer metadataContainer,
      VariantFilterFactory variantFilterFactory, IntegerCounter metadataContextCounter,
      @NonNull SSMValidator<SSMPrimary, SSMPrimaryFieldMapping> ssmPrimaryValidator,
      @NonNull SSMValidator<SSMMetadata, SSMMetadataFieldMapping> ssmMetadataValidator) {
    return new DccProjectProcessor(storageFactory, dccPrimaryTransformerFactory, dccMetadataTransformerFactory,
        metadataContainer,
        variantFilterFactory, metadataContextCounter,
        true,
        true,
        ssmPrimaryValidator,
        ssmMetadataValidator);
  }

  public static DccProjectProcessor newProcessorNoValidation(StorageFactory storageFactory,
      DccTransformerFactory<SSMPrimary> dccPrimaryTransformerFactory,
      DccTransformerFactory<SSMMetadata> dccMetadataTransformerFactory, MetadataContainer metadataContainer,
      VariantFilterFactory variantFilterFactory, IntegerCounter metadataContextCounter) {
    return new DccProjectProcessor(storageFactory, dccPrimaryTransformerFactory, dccMetadataTransformerFactory,
        metadataContainer,
        variantFilterFactory, metadataContextCounter,
        true,
        false,
        null,
        null);
  }

  @NonNull private final StorageFactory storageFactory;
  @NonNull private final DccTransformerFactory<SSMPrimary> dccPrimaryTransformerFactory;
  @NonNull private final DccTransformerFactory<SSMMetadata> dccMetadataTransformerFactory;
  @NonNull private final MetadataContainer metadataContainer;
  @NonNull private final VariantFilterFactory variantFilterFactory;
  @NonNull private final IntegerCounter metadataContextCounter;

  private final boolean enableTSVValidation;
  private final boolean enableSSMValidation;
  private final SSMValidator<SSMPrimary, SSMPrimaryFieldMapping> ssmPrimaryValidator;
  private final SSMValidator<SSMMetadata, SSMMetadataFieldMapping> ssmMetadataValidator;

  @SneakyThrows
  private static void transformSSMPrimary(DccTransformer<SSMPrimary> transformer,
      DccTransformerContext<SSMPrimary> ptx) {
    transformer.transform(ptx);
  }

  @SneakyThrows
  private static void transformSSMMetadata(DccTransformer<SSMMetadata> transformer,
      DccTransformerContext<SSMMetadata> mtx) {
    transformer.transform(mtx);
  }

  private static <T> boolean validateSSM(String typeName, SSMValidator<T, ?> ssmValidator, T ssm) {
    val erroredFieldMappings = ssmValidator.validateFields(ssm);
    if (erroredFieldMappings.isEmpty()) {
      return true;
    } else {
      log.error("[{}_DICTIONARY_VALIDATION]: **FAILED** -> for fields: {}",
          typeName.toUpperCase(),
          erroredFieldMappings
              .stream()
              .map(Object::toString)
              .collect(joining(", ")));
      return false;
    }
  }

  private static void logSSMPrimaryValidationSummary(final boolean enableValidation, final boolean overallPrimaryFileOk, String vcfFilename){
    logSSMValidationSummary("SSM_PRIMARY", enableValidation, overallPrimaryFileOk, vcfFilename);
  }

  private static void logSSMValidationSummary(String typeName, final boolean enableValidation, final boolean overallPrimaryFileOk, String vcfFilename){
    if (enableValidation){
      if (overallPrimaryFileOk){
        log.info("[{}_DICTIONARY_VALIDATION_SUMMARY]: {} --> File[{}] had no errors detected ",typeName.toUpperCase(), PASSED, vcfFilename);
      } else {
        log.error("[{}_DICTIONARY_VALIDATION_SUMMARY]: {} -->File [{}] failed validation with errors ", typeName.toUpperCase(),FAILED, vcfFilename);
      }
    }
  }


  @SneakyThrows
  public void process(String dccProjectCode) {
    val totalMetadataContexts = metadataContainer.getTotalMetadataContexts();
    val storage = storageFactory.getStorage(dccProjectCode);
    val dccMetadataTransformer = dccMetadataTransformerFactory.getDccTransformer(dccProjectCode);
    val dccPrimaryTransformer = dccPrimaryTransformerFactory.getDccTransformer(dccProjectCode);
    val metadataDTCConverter = newMetadataDTCConverter();

    for (val metadataContext : metadataContainer.getMetadataContexts(dccProjectCode)) {
      val portalMetadata = metadataContext.getPortalMetadata();
      metadataContextCounter.incr();
      log.info("");
      log.info("Loading File ( {} / {} ): {}",
          metadataContextCounter.getCount(), totalMetadataContexts, portalMetadata.getPortalFilename().getFilename());
      val sampleMetadata = metadataContext.getSampleMetadata();
      processPortalMetadata(dccPrimaryTransformer, dccProjectCode, storage, portalMetadata, sampleMetadata,
          metadataDTCConverter);
    }

    metadataDTCConverter.convert().forEach(mtx -> transformSSMMetadata(dccMetadataTransformer, mtx));

    dccMetadataTransformer.close();
    dccPrimaryTransformer.close();

    if (enableTSVValidation) {
      validateMetadataOutputFiles(dccMetadataTransformer);
      validatePrimaryOutputFiles(dccPrimaryTransformer);
    }

    //TODO: implements error handling and stats
    //      logFileSummary(erroredFileList, dccProjectCode);
    //      log.info("DccProjectCode[{}] Stats:  TotalConsensusVariants: {} ErroredVariants: {}  ErroredSSMPrimary: {}   TransformedSSMPrimary: {}",
    //          dccProjectCode,
    //          totalVariantCountForProjectCode,
    //          erroredVariantCountForProjectCode,
    //          erroredSSMPrimaryCountForProjectCode,
    //          transformedSSMPrimaryCountForProjectCode);

  }

  private static void validateMetadataOutputFiles(DccTransformer<SSMMetadata> dccMetadataTransformer) {
    for (val path : dccMetadataTransformer.getWrittenPaths()) {
      if (Files.exists(path)) {
        val ssmMValidator = newTsvValidator(path.toString(), SSMMetadataFieldMapping.values().length);
        ssmMValidator.analyze();
        ssmMValidator.log();
      }
    }
  }

  private static void validatePrimaryOutputFiles(DccTransformer<SSMPrimary> dccPrimaryTransformer) {
    for (val path : dccPrimaryTransformer.getWrittenPaths()) {
      if (Files.exists(path)) {
        val ssmPValidator = newTsvValidator(path.toString(), SSMPrimaryFieldMapping.values().length);
        ssmPValidator.analyze();
        ssmPValidator.log();
      }
    }
  }

  @SneakyThrows
  private void processPortalMetadata(DccTransformer<SSMPrimary> dccPrimaryTransformer, String dccProjectCode,
      Storage storage, PortalMetadata portalMetadata, SampleMetadata sampleMetadata, MetadataDTCConverter metadataDTCConverter) {

    try {
      // Download vcfFile
      val vcfFile = storage.getFile(portalMetadata);

      val vcfStreamFilter = newVCFStreamFilter(vcfFile.toPath(), sampleMetadata,variantFilterFactory);
      val consensusVariantProcessor = newConsensusVariantProcessor(sampleMetadata, VARIANT_CONVERTER_STRATEGY_MUX);
      val primaryDTCConverter = newPrimaryDTCConverter(consensusVariantProcessor);
      val primaryCounterMonitor = CounterMonitor.newMonitor("PRIMARY_DTC_CONV", 100000);

      primaryCounterMonitor.start();
      vcfStreamFilter.streamFilteredVariants()
          .map(v -> primaryDTCConverter.convert(v, primaryCounterMonitor)) // Convert variants to PrimaryDTC objects
          .flatMap(Collection::stream)
          .filter(this::shouldTransformSSMPrimary)
          .map(pdtc -> metadataDTCConverter.accumulatePrimaryDTC(sampleMetadata, pdtc) ) // Accumulate data for later MetadataDTC creation, primary DTC just pass through
          .forEach(ptx -> transformSSMPrimary(dccPrimaryTransformer, ptx));
      primaryCounterMonitor.stop();

    } catch (Exception e) {
      log.error("[{}]: {}\n{}", e.getClass().getSimpleName(), e.getMessage(), NEWLINE.join(e.getStackTrace()));
    }

  }

  private boolean shouldTransformSSMPrimary(DccTransformerContext<SSMPrimary> ptx) {
    boolean shouldTransformSSMPrimary = true;
    if (enableSSMValidation) {
      shouldTransformSSMPrimary = validateSSM("SSM_PRIMARY", ssmPrimaryValidator, ptx.getObject());
    }
    return shouldTransformSSMPrimary;
  }

}
