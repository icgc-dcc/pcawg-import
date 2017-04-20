/*
 * Copyright (c) 2017 The Ontario Institute for Cancer Research. All rights reserved.
 *
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with
 * this program. If not, see <http://www.gnu.org/licenses/>.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.pcawg.client;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.ConsensusVCFConverterFactory;
import org.icgc.dcc.pcawg.client.core.DccTransformerFactory;
import org.icgc.dcc.pcawg.client.core.StorageFactory;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformer;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.data.portal.PortalMetadata;
import org.icgc.dcc.pcawg.client.download.LocalStorageFileNotFoundException;
import org.icgc.dcc.pcawg.client.download.MetadataContainer;
import org.icgc.dcc.pcawg.client.download.Storage;
import org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory;
import org.icgc.dcc.pcawg.client.model.ssm.SSMCommon;
import org.icgc.dcc.pcawg.client.model.ssm.SSMValidator;
import org.icgc.dcc.pcawg.client.model.ssm.classification.SSMClassification;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadataFieldMapping;
import org.icgc.dcc.pcawg.client.model.ssm.primary.FieldExtractor;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimaryFieldMapping;
import org.icgc.dcc.pcawg.client.utils.measurement.IntegerCounter;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVCFException;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.icgc.dcc.common.core.util.Joiners.NEWLINE;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.pcawg.client.Importer.DccProjectProcessor.newProcessorNoValidation;
import static org.icgc.dcc.pcawg.client.Importer.STATS_FIELDS.NUM_TRANSFORMED;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.PERSISTANCE_DIR;
import static org.icgc.dcc.pcawg.client.core.ConsensusVCFConverterFactory.newConsensusVCFConverterFactory;
import static org.icgc.dcc.pcawg.client.core.Factory.buildDictionaryCreator;
import static org.icgc.dcc.pcawg.client.core.Factory.newDccMetadataTransformerFactory;
import static org.icgc.dcc.pcawg.client.core.Factory.newDccPrimaryTransformerFactory;
import static org.icgc.dcc.pcawg.client.core.Factory.newFsController;
import static org.icgc.dcc.pcawg.client.core.Factory.newSSMMetadataValidator;
import static org.icgc.dcc.pcawg.client.core.Factory.newSSMPrimaryValidator;
import static org.icgc.dcc.pcawg.client.core.PersistedFactory.newPersistedFactory;
import static org.icgc.dcc.pcawg.client.download.LocalStorage.newLocalStorage;
import static org.icgc.dcc.pcawg.client.download.PortalStorage.newPortalStorage;
import static org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory.newVariantFilterFactory;
import static org.icgc.dcc.pcawg.client.tsv.TsvValidator.newTsvValidator;
import static org.icgc.dcc.pcawg.client.utils.measurement.CounterMonitor.newMonitor;
import static org.icgc.dcc.pcawg.client.utils.measurement.IntegerCounter.newDefaultIntegerCounter;

@Slf4j
@Builder
public class Importer implements Runnable {

  private static final boolean REQUIRE_INDEX_CFG = false;
  private static final boolean ENABLE_TSV_VALIDATION = true;
  private static final boolean ENABLE_SSM_DICTIONARY_VALIDATION = false;
  private static final String FAILED = "**FAILED**";
  private static final String PASSED = "**PASSED**";

  @NonNull
  private final String token;
  private final boolean hdfsEnabled;

  @NonNull
  private final String outputVcfDir;
  private final boolean persistVcfDownloads;
  private final boolean bypassMD5Check;

  @NonNull
  private final String outputTsvDir;

  @NonNull
  private final Optional<String> optionalHdfsHostname;

  @NonNull
  private final Optional<String> optionalHdfsPort;

  @NonNull private final boolean useCollab;
  @NonNull private final boolean bypassTcgaFiltering;
  @NonNull private final boolean bypassNoiseFiltering;

  /**
   * State
   */
  private DccTransformerFactory<SSMPrimary> dccPrimaryTransformerFactory;
  private DccTransformerFactory<SSMMetadata> dccMetadataTransformerFactory;
  private MetadataContainer metadataContainer;
  private SSMValidator<SSMPrimary, SSMPrimaryFieldMapping> ssmPrimaryValidator;
  private SSMValidator<SSMMetadata, SSMMetadataFieldMapping> ssmMetadataValidator;
  private ConsensusVCFConverterFactory consensusVCFConverterFactory;
  private boolean isInitDccTransformerFactory = false;
  private boolean isInitMetadataContainer = false;
  private boolean isInitSSMValidators = false;
  private boolean isInitConsensusVCFConverterFactory = false;

  private DccProjectCodeStats2 dccStats = new DccProjectCodeStats2();

  private  Storage newStorage(String dccProjectCode){
    if(useCollab){
      val vcfDownloadDirectory = PATH.join(outputVcfDir, dccProjectCode);
      return newPortalStorage(persistVcfDownloads, vcfDownloadDirectory, bypassMD5Check, token);
    } else {
      return newLocalStorage(Paths.get(outputVcfDir),bypassMD5Check);
    }
  }

  private void initDccTransformerFactory(){
    val fsController = newFsController(hdfsEnabled, optionalHdfsHostname, optionalHdfsPort);
    dccPrimaryTransformerFactory = newDccPrimaryTransformerFactory(fsController, outputTsvDir);
    dccMetadataTransformerFactory = newDccMetadataTransformerFactory(fsController, outputTsvDir);
    this.isInitDccTransformerFactory = true;
  }

  private void initMetadataContainer(){
    val persistDirPath = Paths.get(PERSISTANCE_DIR);
    val persistedFactory = newPersistedFactory(persistDirPath, true);
    // Create container with all MetadataContexts
    log.info("Creating MetadataContainer");
    metadataContainer = persistedFactory.newMetadataContainer(useCollab);
    this.isInitMetadataContainer = true;
  }

  private void initSSMValidators(){
    val dictionaryCreator = buildDictionaryCreator();
    ssmPrimaryValidator = newSSMPrimaryValidator(dictionaryCreator.getSSMPrimaryFileSchema());
    ssmMetadataValidator = newSSMMetadataValidator(dictionaryCreator.getSSMMetadataFileSchema());
    isInitSSMValidators = true;
  }

  private void init(){
    initDccTransformerFactory();
    initMetadataContainer();
//    initSSMValidators();
  }

  @SneakyThrows
  @Override
  public void run() {
    init();
    val totalDccProjectCodes = metadataContainer.getDccProjectCodes().size();
    int countDccProjectCodes  = 0;

    val metadataContextCounter = newDefaultIntegerCounter();
    val variantFilterFactory = newVariantFilterFactory(bypassTcgaFiltering, bypassNoiseFiltering);
    val storageFactory = StorageFactory.builder()
        .bypassMD5Check(bypassMD5Check)
        .outputVcfDir(Paths.get(outputVcfDir))
        .persistVcfDownloads(persistVcfDownloads)
        .token(token)
        .useCollab(useCollab)
        .build();

    // Loop through each dccProjectCode
    for (val dccProjectCode : metadataContainer.getDccProjectCodes()) {
      log.info("Processing DccProjectCode ( {} / {} ): {}",
          ++countDccProjectCodes, totalDccProjectCodes, dccProjectCode);
      val storage = newStorage(dccProjectCode);
      val processor = newProcessorNoValidation(storageFactory, dccPrimaryTransformerFactory, dccMetadataTransformerFactory,
          metadataContainer,variantFilterFactory, metadataContextCounter);
      processor.process(dccProjectCode);

    }
    variantFilterFactory.close();
  }

  @RequiredArgsConstructor
  public static class DccProjectProcessor {

    public static DccProjectProcessor newProcessorWithValidation( StorageFactory storageFactory, DccTransformerFactory<SSMPrimary> dccPrimaryTransformerFactory,
        DccTransformerFactory<SSMMetadata> dccMetadataTransformerFactory, MetadataContainer metadataContainer,
        VariantFilterFactory variantFilterFactory, IntegerCounter metadataContextCounter,
        @NonNull SSMValidator<SSMPrimary, SSMPrimaryFieldMapping> ssmPrimaryValidator,
        @NonNull SSMValidator<SSMMetadata, SSMMetadataFieldMapping> ssmMetadataValidator){
      return new DccProjectProcessor(storageFactory, dccPrimaryTransformerFactory, dccMetadataTransformerFactory, metadataContainer,
          variantFilterFactory, metadataContextCounter,
    true,
    true,
          ssmPrimaryValidator,
          ssmMetadataValidator);
    }

    public static DccProjectProcessor newProcessorNoValidation( StorageFactory storageFactory, DccTransformerFactory<SSMPrimary> dccPrimaryTransformerFactory,
        DccTransformerFactory<SSMMetadata> dccMetadataTransformerFactory, MetadataContainer metadataContainer,
        VariantFilterFactory variantFilterFactory, IntegerCounter metadataContextCounter){
      return new DccProjectProcessor(storageFactory, dccPrimaryTransformerFactory, dccMetadataTransformerFactory, metadataContainer,
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
    public void process(String dccProjectCode){
      val totalMetadataContexts = metadataContainer.getTotalMetadataContexts();
      val storage = storageFactory.getStorage(dccProjectCode);
      val dccMetadataTransformer = dccMetadataTransformerFactory.getDccTransformer(dccProjectCode);
      val dccPrimaryTransformer = dccPrimaryTransformerFactory.getDccTransformer(dccProjectCode);

      val ssmMetadataDTCSet = Sets.<DccTransformerContext<SSMMetadata>>newHashSet();

      for(val metadataContext : metadataContainer.getMetadataContexts(dccProjectCode)){
        val portalMetadata = metadataContext.getPortalMetadata();
        metadataContextCounter.incr();
        log.info("");
        log.info("Loading File ( {} / {} ): {}",
            metadataContextCounter.getCount(), totalMetadataContexts, portalMetadata.getPortalFilename().getFilename());
        val consensusSampleMetadata = metadataContext.getSampleMetadata();
        processPortalMetadata(dccPrimaryTransformer,dccProjectCode, storage,portalMetadata, consensusSampleMetadata, ssmMetadataDTCSet);
      }

      //Transform SSMMetadatas based on unique list of SSMClassifications
      ssmMetadataDTCSet.forEach(mtx -> transformSSMMetadata(dccMetadataTransformer, mtx));

      dccMetadataTransformer.close();
      dccPrimaryTransformer.close();

      if (enableTSVValidation){
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

    private static void validateMetadataOutputFiles(DccTransformer<SSMMetadata> dccMetadataTransformer){
      for (val path : dccMetadataTransformer.getWrittenPaths()){
        if (Files.exists(path)){
          val ssmMValidator = newTsvValidator(path.toString(), SSMMetadataFieldMapping.values().length);
          ssmMValidator.analyze();
          ssmMValidator.log();
        }
      }
    }

    private static void validatePrimaryOutputFiles(DccTransformer<SSMPrimary> dccPrimaryTransformer){
      for (val path : dccPrimaryTransformer.getWrittenPaths()){
        if (Files.exists(path)){
          val ssmPValidator = newTsvValidator(path.toString(), SSMPrimaryFieldMapping.values().length);
          ssmPValidator.analyze();
          ssmPValidator.log();
        }
      }
    }

    @SneakyThrows
    private void processPortalMetadata(DccTransformer<SSMPrimary> dccPrimaryTransformer, String dccProjectCode, Storage storage, PortalMetadata portalMetadata, SampleMetadata consensusSampleMetadata, Set<DccTransformerContext<SSMMetadata>> ssmMetadataDTCSet){

      val ssmClassificationSet = Sets.<SSMClassification>newHashSet();
      try {
        // Download vcfFile
        val vcfFile = storage.getFile(portalMetadata);

        // Convert Consensus VCF files
        val consensusVCFConverterFactory = newConsensusVCFConverterFactory(vcfFile.toPath(), consensusSampleMetadata,variantFilterFactory);
        val consensusSSMMetadataConverter = consensusVCFConverterFactory.getConsensusSSMMetadataConverter();
        val consensusSSMPrimaryConverter = consensusVCFConverterFactory.getConsensusSSMPrimaryConverter();


        val primaryCounterMonitor = newMonitor("primaryCounterMonitor", 100000);

        primaryCounterMonitor.start();
        consensusSSMPrimaryConverter.streamSSMPrimary(primaryCounterMonitor)
            .filter(this::shouldTransformSSMPrimary)
            .map(x -> aggregateSSMClassification(ssmClassificationSet, x) )
            .forEach(ptx -> transformSSMPrimary(dccPrimaryTransformer, ptx));
        primaryCounterMonitor.stop();


        ssmMetadataDTCSet.addAll(consensusSSMMetadataConverter.convert());

      }  catch (LocalStorageFileNotFoundException e){
        log.error("[{}]: {}\n{}", e.getClass().getSimpleName(), e.getMessage(), NEWLINE.join(e.getStackTrace()));
      } catch (PcawgVCFException e) {
        //TODO: do error calculation and stats here
      }


    }

    private static DccTransformerContext<SSMPrimary> aggregateSSMClassification(Set<SSMClassification> ssmPrimaryClassificationSet, DccTransformerContext<SSMPrimary> ssmPrimaryDccTransformerContext){
      ssmPrimaryClassificationSet.add(ssmPrimaryDccTransformerContext.getSSMClassification());
      return ssmPrimaryDccTransformerContext;
    }




    private boolean shouldTransformSSMPrimary(DccTransformerContext<SSMPrimary> ptx){
      boolean shouldTransformSSMPrimary = true;
      if (enableSSMValidation) {
        shouldTransformSSMPrimary = validateSSM("SSM_PRIMARY", ssmPrimaryValidator, ptx.getObject());
      }
      return shouldTransformSSMPrimary;
    }

  }

  @SneakyThrows
  private static void transformSSMPrimary(DccTransformer<SSMPrimary> transformer, DccTransformerContext<SSMPrimary> ptx){
    transformer.transform(ptx);
  }

  @SneakyThrows
  private static void transformSSMMetadata(DccTransformer<SSMMetadata> transformer, DccTransformerContext<SSMMetadata> mtx){
    transformer.transform(mtx);
  }

  private boolean shouldTransformSSMPrimary(DccTransformerContext<SSMPrimary> ptx){
    boolean shouldTransformSSMPrimary = true;
    if (isInitSSMValidators && ENABLE_SSM_DICTIONARY_VALIDATION) {
      shouldTransformSSMPrimary = validateSSM("SSM_PRIMARY", ssmPrimaryValidator, ptx.getObject());
    }
    return shouldTransformSSMPrimary;
  }

  enum SSM_TYPE {
    SSM_PRIMARY,
    SSM_METADATA;
  }
  private static class DccProjectCodeStats2 {

    private final Map<String, ProcStats[]> map = Maps.newHashMap();

    public ProcStats get(String dccProjectCode, SSM_TYPE ssmType){
      if (!map.containsKey(dccProjectCode)){
        val procStatArray = new ProcStats[SSM_TYPE.values().length];
        int i=0;
        for(val type : SSM_TYPE.values()){
          procStatArray[i] = new ProcStats();
          i++;
        }
        map.put(dccProjectCode, procStatArray);
      }
      return map.get(dccProjectCode)[ssmType.ordinal()];
    }
  }

  private static class DccProjectCodeStats {

    private final Map<String, ProcStats> map = Maps.newHashMap();

    public ProcStats get(String dccProjectCode){
      if (!map.containsKey(dccProjectCode)){
        map.put(dccProjectCode, new ProcStats());
      }
      return map.get(dccProjectCode);
    }
  }

  enum STATS_FIELDS {
    NUM_TRANSFORMED,
    TOTAL_VARIANT_COUNT,
    NUM_VCF_FILES_ERRORED,
    NUM_SSM_PRIMARY_ERRORED;
  }

  private static class ProcStats {


    private final int[] array = new int[STATS_FIELDS.values().length];

    private void set(STATS_FIELDS field, int val){
      array[field.ordinal()] = val;
    }

    private int get(STATS_FIELDS field){
     return array[field.ordinal()];
    }

    private int incr(STATS_FIELDS field, int val){
      array[field.ordinal()] += val;
      return array[field.ordinal()];
    }

    private int incr(STATS_FIELDS field){
      return incr(field, 1);
    }

    public void reset(){
      for (val field : STATS_FIELDS.values()){
        set(field, 0);
      }
    }

  }

  @SneakyThrows
  private <T, F extends FieldExtractor<T>> void process(String typeName, String vcfFilename,  DccTransformer<T> dccTransformer, List<DccTransformerContext<T>> data, SSMValidator<T, F> ssmValidator, ProcStats stats){
    boolean overallPrimaryFileOk = true;
    for (val ptx : data){
      boolean shouldTransformSSM = true;
      if (ENABLE_SSM_DICTIONARY_VALIDATION){
        shouldTransformSSM = validateSSM(typeName, ssmValidator, ptx.getObject());
      }
      if (shouldTransformSSM){
        dccTransformer.transform(ptx);

        //Record number of SSM Primary object transformeed
        stats.incr(NUM_TRANSFORMED);
      }
      overallPrimaryFileOk &= shouldTransformSSM;
    }
    logSSMValidationSummary(typeName, ENABLE_SSM_DICTIONARY_VALIDATION, overallPrimaryFileOk, vcfFilename);
  }

  private static <T> boolean validateSSM(String typeName, SSMValidator<T, ? > ssmValidator, T ssm){
    val erroredFieldMappings = ssmValidator.validateFields(ssm);
    if (erroredFieldMappings.isEmpty()){
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

  private static void logFileSummary(List<String> list, String dccProjectCode){
    if (!list.isEmpty()){
      log.error("[FILE_SUMMARY]: **{}** DccProjectCode[{}] - The importer FAILED to import all vcf files. The following files were problematic:\n{}",
          FAILED,
          dccProjectCode,
          list.stream()
              .collect(joining("\n")));
    } else {
      log.info("[FILE_SUMMARY]: **{}** DccProjectCode[{}] - The importer SUCCESSFULLY imported all vcf files.", PASSED, dccProjectCode);
    }
  }


  private static void validateCommon(String dccProjectCode,String filename, Iterable<DccTransformerContext<SSMMetadata>> ssmMetadataList, Iterable<DccTransformerContext<SSMPrimary>> ssmPrimaryList){
    val ssmMList = stream(ssmMetadataList).map(DccTransformerContext::getObject).collect(toList());
    val ssmPList = stream(ssmPrimaryList).map(DccTransformerContext::getObject).collect(toList());
    val diffList = SSMValidator.differenceOfMetadataAndPrimary(ssmMList, ssmPList);
    if (!diffList.isEmpty()){
      log.error("[VALIDATE_COMMON_RESULT]: {} -> For DccProjectCode[{}] and File[{}], the following are mismatched: [{}]", FAILED, dccProjectCode,
          filename,
          diffList.stream().map(SSMCommon::getString).collect(java.util.stream.Collectors.joining(" , ")));
    } else {
      log.info("[VALIDATE_COMMON_RESULT]: {} -> For DccProjectCode[{}] and File[{}]",PASSED, dccProjectCode, filename);
    }
  }

}
