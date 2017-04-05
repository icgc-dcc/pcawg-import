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
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.Factory;
import org.icgc.dcc.pcawg.client.core.fscontroller.FsController;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformer;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadataFieldMapping;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimaryFieldMapping;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVCFException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.pcawg.client.core.Factory.newFsController;
import static org.icgc.dcc.pcawg.client.core.Factory.newMetadataContainer;
import static org.icgc.dcc.pcawg.client.download.Storage.newStorage;
import static org.icgc.dcc.pcawg.client.tsv.TsvValidator.newTsvValidator;
import static org.icgc.dcc.pcawg.client.vcf.ConsensusVCFConverter2.newConsensusVCFConverter2;

@Slf4j
@Builder
public class Importer implements Runnable {

  private static final boolean REQUIRE_INDEX_CFG = false;
  private static final boolean ENABLE_TSV_VALIDATION = true;

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


    val totalMetadataContexts = metadataContainer.getTotalMetadataContexts();
    int countMetadataContexts = 0;

    val totalDccProjectCodes = metadataContainer.getDccProjectCodes().size();
    int countDccProjectCodes  = 0;
    val erroredFileList =  Lists.<String>newArrayList();

    // Loop through each dccProjectCode
    for (val dccProjectCode : metadataContainer.getDccProjectCodes()) {
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

        log.info("Loading File ( {} / {} ): {}",
            ++countMetadataContexts, totalMetadataContexts, portalMetadata.getPortalFilename().getFilename());

        // Download vcfFile
        val vcfFile = storage.downloadFile(portalMetadata);

        // Get consensusSampleMetadata
        val consensusSampleMetadata = metadataContext.getSampleMetadata();

        // Convert Consensus VCF files
        try{
          val converter = newConsensusVCFConverter2(vcfFile.toPath(), consensusSampleMetadata);
          for (val mtx : converter.readSSMMetadata()){
            dccMetadataTransformer.transform(mtx);
          }

          for (val ptx : converter.readSSMPrimary()){
            dccPrimaryTransformer.transform(ptx);
          }

        } catch (PcawgVCFException e){
          erroredFileList.add(vcfFile.getAbsolutePath());
        }
      }
      dccMetadataTransformer.close();
      dccPrimaryTransformer.close();
      if (ENABLE_TSV_VALIDATION){
        validateOutputFiles(dccMetadataTransformer, dccPrimaryTransformer);
      }
    }
    checkFileErrors(erroredFileList);
  }

  private static void checkFileErrors(List<String> list){
    if (!list.isEmpty()){
      log.error("The importer FAILED to import all vcf files. The following files were problematic:\n{}",
          list.stream()
              .collect(joining("\n")));
    } else {
      log.info("The importer SUCCESSFULLY imported all vcf files.");
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
