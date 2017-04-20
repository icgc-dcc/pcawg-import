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

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.VCFConverterFactory;
import org.icgc.dcc.pcawg.client.core.DccTransformerFactory;
import org.icgc.dcc.pcawg.client.core.StorageFactory;
import org.icgc.dcc.pcawg.client.download.MetadataContainer;
import org.icgc.dcc.pcawg.client.model.ssm.SSMValidator;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadataFieldMapping;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimaryFieldMapping;

import java.nio.file.Paths;
import java.util.Optional;

import static org.icgc.dcc.pcawg.client.DccProjectProcessor.newProcessorNoValidation;
import static org.icgc.dcc.pcawg.client.DccProjectProcessor.newProcessorWithValidation;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.PERSISTANCE_DIR;
import static org.icgc.dcc.pcawg.client.core.Factory.buildDictionaryCreator;
import static org.icgc.dcc.pcawg.client.core.Factory.newDccMetadataTransformerFactory;
import static org.icgc.dcc.pcawg.client.core.Factory.newDccPrimaryTransformerFactory;
import static org.icgc.dcc.pcawg.client.core.Factory.newFsController;
import static org.icgc.dcc.pcawg.client.core.Factory.newSSMMetadataValidator;
import static org.icgc.dcc.pcawg.client.core.Factory.newSSMPrimaryValidator;
import static org.icgc.dcc.pcawg.client.core.PersistedFactory.newPersistedFactory;
import static org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory.newVariantFilterFactory;
import static org.icgc.dcc.pcawg.client.utils.measurement.IntegerCounter.newDefaultIntegerCounter;

@Slf4j
@Builder
public class Importer implements Runnable {

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
  @NonNull private final boolean enableSSMValidation;

  /**
   * State
   */
  private DccTransformerFactory<SSMPrimary> dccPrimaryTransformerFactory;
  private DccTransformerFactory<SSMMetadata> dccMetadataTransformerFactory;
  private MetadataContainer metadataContainer;
  private SSMValidator<SSMPrimary, SSMPrimaryFieldMapping> ssmPrimaryValidator;
  private SSMValidator<SSMMetadata, SSMMetadataFieldMapping> ssmMetadataValidator;
  private VCFConverterFactory VCFConverterFactory;
  private boolean isInitDccTransformerFactory = false;
  private boolean isInitMetadataContainer = false;
  private boolean isInitSSMValidators = false;
  private boolean isInitConsensusVCFConverterFactory = false;

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
    if (enableSSMValidation){
      initSSMValidators();
    }
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

      DccProjectProcessor processor = null;
      if (enableSSMValidation){
        processor = newProcessorWithValidation(storageFactory,dccPrimaryTransformerFactory,
            dccMetadataTransformerFactory,metadataContainer,variantFilterFactory, metadataContextCounter,ssmPrimaryValidator, ssmMetadataValidator);
      } else {
        processor = newProcessorNoValidation(storageFactory, dccPrimaryTransformerFactory, dccMetadataTransformerFactory,
            metadataContainer,variantFilterFactory, metadataContextCounter);
      }
      processor.process(dccProjectCode);

    }
    variantFilterFactory.close();
  }

}
