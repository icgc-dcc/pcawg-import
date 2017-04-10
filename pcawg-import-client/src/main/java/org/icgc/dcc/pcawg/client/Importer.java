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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.Factory;
import org.icgc.dcc.pcawg.client.core.fscontroller.FsController;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformer;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.model.ssm.SSMValidator;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadataFieldMapping;
import org.icgc.dcc.pcawg.client.model.ssm.primary.FieldExtractor;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimaryFieldMapping;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVCFException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.joining;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.pcawg.client.Importer.STATS_FIELDS.NUM_TRANSFORMED;
import static org.icgc.dcc.pcawg.client.core.Factory.buildDictionaryCreator;
import static org.icgc.dcc.pcawg.client.core.Factory.newFsController;
import static org.icgc.dcc.pcawg.client.core.Factory.newMetadataContainer;
import static org.icgc.dcc.pcawg.client.core.Factory.newSSMMetadataValidator;
import static org.icgc.dcc.pcawg.client.core.Factory.newSSMPrimaryValidator;
import static org.icgc.dcc.pcawg.client.download.Storage.newStorage;
import static org.icgc.dcc.pcawg.client.tsv.TsvValidator.newTsvValidator;
import static org.icgc.dcc.pcawg.client.vcf.ConsensusVCFConverter.newConsensusVCFConverter;

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

  /**
   * State
   */
  private DccProjectCodeStats2 dccStats = new DccProjectCodeStats2();

  private DccTransformer<SSMMetadata> buildDccMetadataTransformer(FsController<Path> fsController, String dccProjectCode){
    return Factory.newDccMetadataTransformer(fsController, this.outputTsvDir, dccProjectCode);
  }
  private DccTransformer<SSMPrimary> buildDccPrimaryTransformer(FsController<Path> fsController, String dccProjectCode){
    return Factory.newDccPrimaryTransformer(fsController, this.outputTsvDir, dccProjectCode);
  }

  @Override
  @SneakyThrows
  public void run() {
    val fsController = newFsController(hdfsEnabled, optionalHdfsHostname, optionalHdfsPort);
    // Create container with all MetadataContexts
    log.info("Creating MetadataContainer");
    val metadataContainer = newMetadataContainer();
    val dictionaryCreator = buildDictionaryCreator();
    val ssmPrimaryValidator = newSSMPrimaryValidator(dictionaryCreator.getSSMPrimaryFileSchema());
    val ssmMetadataValidator = newSSMMetadataValidator(dictionaryCreator.getSSMMetadataFileSchema());


    val totalMetadataContexts = metadataContainer.getTotalMetadataContexts();
    int countMetadataContexts = 0;

    val totalDccProjectCodes = metadataContainer.getDccProjectCodes().size();
    int countDccProjectCodes  = 0;

    // Loop through each dccProjectCode
    for (val dccProjectCode : metadataContainer.getDccProjectCodes()) {
      // Error counters
      int totalVariantCountForProjectCode = 0;
      int transformedSSMPrimaryCountForProjectCode = 0;
      int erroredSSMPrimaryCountForProjectCode = 0;
      val erroredFileList =  Lists.<String>newArrayList();
      //rtismaFIX  val stats = dccStats.get(dccProjectCode);

      log.info("Processing DccProjectCode ( {} / {} ): {}",
          ++countDccProjectCodes, totalDccProjectCodes, dccProjectCode);

      // Create storage manager for downloading files
      val vcfDownloadDirectory = PATH.join(outputVcfDir, dccProjectCode);
      val storage = newStorage(persistVcfDownloads, vcfDownloadDirectory , bypassMD5Check, token);
      val dccPrimaryTransformer = buildDccPrimaryTransformer(fsController,dccProjectCode);
      val dccMetadataTransformer = buildDccMetadataTransformer(fsController,dccProjectCode);

      // Loop through each file for a particular dccProjectCode
      for (val metadataContext : metadataContainer.getMetadataContexts(dccProjectCode)) {
        val portalMetadata = metadataContext.getPortalMetadata();

        log.info("");
        log.info("Loading File ( {} / {} ): {}",
            ++countMetadataContexts, totalMetadataContexts, portalMetadata.getPortalFilename().getFilename());

        // Download vcfFile
        val vcfFile = storage.downloadFile(portalMetadata);

        // Get consensusSampleMetadata
        val consensusSampleMetadata = metadataContext.getSampleMetadata();

        // Convert Consensus VCF files
        val consensusVCFConverter = newConsensusVCFConverter(vcfFile.toPath(), consensusSampleMetadata);
        try{
          consensusVCFConverter.process();
        } catch (PcawgVCFException e){
          erroredFileList.add(vcfFile.getAbsolutePath());

          // Record number of errored variants
          erroredSSMPrimaryCountForProjectCode += consensusVCFConverter.getBadSSMPrimaryCount();

          //rtismaFIX stats.incr(NUM_SSM_PRIMARY_ERRORED, consensusVCFConverter.getBadSSMPrimaryCount());
          //rtismaFIX stats.incr(NUM_VCF_FILES_ERRORED);
        }

        //Record number of total variants processed
       totalVariantCountForProjectCode += consensusVCFConverter.getVariantCount();
      //rtismaFIX        stats.incr(TOTAL_VARIANT_COUNT, consensusVCFConverter.getVariantCount());


        //rtismaFIX process(SSM_METADATA.name(),vcfFile.getPath(),
        //rtismaFIX     dccMetadataTransformer,Lists.newArrayList(consensusVCFConverter.readSSMMetadata()),
        //rtismaFIX     ssmMetadataValidator,dccMetadataStats);

        //rtismaFIX process(SSM_PRIMARY.name(),vcfFile.getPath(),
        //rtismaFIX     dccPrimaryTransformer,consensusVCFConverter.readSSMPrimary(),
        //rtismaFIX    ssmPrimaryValidator,dccPrimaryStats);

        // SSM Metadata transformation
        for (val mtx : consensusVCFConverter.readSSMMetadata()){

          dccMetadataTransformer.transform(mtx);
        }

        // SSM Primary transformation
        boolean overallPrimaryFileOk = true;
        val ssmPrimaryList = consensusVCFConverter.readSSMPrimary();
        for (val ptx : ssmPrimaryList){
          boolean shouldTransformSSMPrimary = true;
          if (ENABLE_SSM_DICTIONARY_VALIDATION){
            shouldTransformSSMPrimary = validateSSM("SSM_PRIMARY", ssmPrimaryValidator, ptx.getObject());
          }
          if (shouldTransformSSMPrimary){
            dccPrimaryTransformer.transform(ptx);
            //Record number of SSM Primary object transformeed
            transformedSSMPrimaryCountForProjectCode++;
          }
          overallPrimaryFileOk &= shouldTransformSSMPrimary;
        }

        logSSMPrimaryValidationSummary(ENABLE_TSV_VALIDATION, overallPrimaryFileOk, vcfFile.getPath());

      }
      dccMetadataTransformer.close();
      dccPrimaryTransformer.close();
      if (ENABLE_TSV_VALIDATION){
        validateOutputFiles(dccMetadataTransformer, dccPrimaryTransformer);
      }
      checkFileErrors(erroredFileList, totalVariantCountForProjectCode, dccProjectCode);
      log.info("DccProjectCode[{}] Stats:  TotalVariants: {}   ErroredSSMPrimary: {}   TransformedSSMPrimary: {}",
          dccProjectCode,
          totalVariantCountForProjectCode,
          erroredSSMPrimaryCountForProjectCode,
          transformedSSMPrimaryCountForProjectCode);

      //rtismaFIX  checkFileErrors(erroredFileList, stats.get(TOTAL_VARIANT_COUNT), dccProjectCode);
      //rtismaFIX  log.info("DccProjectCode[{}] Stats:  TotalVariants: {}   ErroredSSMPrimary: {}   TransformedSSMPrimary: {}",
      //rtismaFIX      dccProjectCode,
      //rtismaFIX      stats.get(TOTAL_VARIANT_COUNT),
      //rtismaFIX      stats.get(NUM_SSM_PRIMARY_ERRORED),
      //rtismaFIX      stats.get(NUM_TRANSFORMED));
    }
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

  private static void checkFileErrors(List<String> list, final int totalBadVariantCount, String dccProjectCode){
    if (!list.isEmpty()){
      log.error("The importer FAILED to import all vcf files. The following files were problematic:\n{}",
          list.stream()
              .collect(joining("\n")));
    } else {
      log.info("The importer SUCCESSFULLY imported all vcf files.");
    }
    if (totalBadVariantCount != 0){
      log.error("Total Bad Variants for DccProjectCode[{}]: {}", dccProjectCode, totalBadVariantCount);
    }

  }

  private static void validateOutputFiles(DccTransformer<SSMMetadata> dccMetadataTransformer, DccTransformer<SSMPrimary> dccPrimaryTransformer){
    for (val path : dccMetadataTransformer.getWrittenPaths()){
      if (Files.exists(path)){
        val ssmMValidator = newTsvValidator(path.toString(), SSMMetadataFieldMapping.values().length);
        ssmMValidator.analyze();
        ssmMValidator.log();
      }
    }
    for (val path : dccPrimaryTransformer.getWrittenPaths()){
      if (Files.exists(path)){
        val ssmPValidator = newTsvValidator(path.toString(), SSMPrimaryFieldMapping.values().length);
        ssmPValidator.analyze();
        ssmPValidator.log();
      }
    }
  }

}
