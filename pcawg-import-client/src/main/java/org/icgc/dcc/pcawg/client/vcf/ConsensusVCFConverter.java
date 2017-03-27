package org.icgc.dcc.pcawg.client.vcf;

import com.google.common.collect.Sets;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.Transformer;
import org.icgc.dcc.pcawg.client.core.TransformerFactory;
import org.icgc.dcc.pcawg.client.core.writer.FileWriterContextFactory;
import org.icgc.dcc.pcawg.client.model.metadata.project.SampleMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadataFieldMapping;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.impl.PlainSSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimaryFieldMapping;
import org.icgc.dcc.pcawg.client.model.ssm.primary.impl.PlainSSMPrimary;

import java.io.File;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Maps.newEnumMap;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.pcawg.client.core.Factory.newSSMMetadata;
import static org.icgc.dcc.pcawg.client.model.NACodes.DATA_VERIFIED_TO_BE_UNKNOWN;
import static org.icgc.dcc.pcawg.client.model.ssm.primary.impl.IndelPcawgSSMPrimary.newIndelSSMPrimary;
import static org.icgc.dcc.pcawg.client.model.ssm.primary.impl.SnvMnvPcawgSSMPrimary.newSnvMnvSSMPrimary;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.INDEL;
import static org.icgc.dcc.pcawg.client.vcf.DataTypes.SNV_MNV;
import static org.icgc.dcc.pcawg.client.vcf.VCF.streamCallers;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;

@Builder
@Slf4j
public class ConsensusVCFConverter {

  private static final boolean REQUIRE_INDEX_CFG = false;
  private static final boolean F_CHECK_CORRECT_WORKTYPE = false;
  private static final boolean DO_VALIDATION = true;

  @NonNull private final TransformerFactory<SSMPrimary> primaryTransformerFactory;
  @NonNull private final FileWriterContextFactory primaryFWCtxFactory;

  @NonNull private final TransformerFactory<SSMMetadata> metadataTransformerFactory;
  @NonNull private final FileWriterContextFactory metadataFWCtxFactory;

  private static final String ILLEGAL_VALUE_REGEX = "^\\s+$";
  /**
   * State
   */
  private Map<WorkflowTypes , Transformer<SSMPrimary>> primaryTransformerMap;
  private Map<WorkflowTypes , Transformer<SSMMetadata>> metadataTransformerMap;

  private static long countIllegalValues(SSMMetadata ssmMetadata){
    return stream(SSMMetadataFieldMapping.values())
        .map( x -> x.extractStringValue(ssmMetadata))
        .filter(s -> (s==null || s.matches(ILLEGAL_VALUE_REGEX)))
        .count();
  }

  private static long countIllegalValues(SSMPrimary ssmPrimary){
    return stream(SSMPrimaryFieldMapping.values())
        .map( x -> x.extractStringValue(ssmPrimary))
        .filter(s -> (s==null || s.matches(ILLEGAL_VALUE_REGEX)))
        .count();
  }

  private static void checkForIllegalMetadata(WorkflowTypes workflowType, String filename, SSMMetadata ssmMetadata){
    val count = countIllegalValues(ssmMetadata);
    if (count > 0){
      log.error("The ssmMetadata({}) file {} has {} null values", workflowType.getName(), filename, count);
    }
  }

  public void process(File vcfFile, SampleMetadata sampleMetadataConsensus){
    checkArgument(sampleMetadataConsensus.getWorkflowType() == CONSENSUS,
        "This method can only process vcf files of workflow type [%s]", CONSENSUS.getName());
    val dccProjectCode = sampleMetadataConsensus.getDccProjectCode();

    // Initialize transformer maps for primary and metadata
    buildTransformerMaps(dccProjectCode);

    // Write SSM Primary to file
    val vcf = new VCFFileReader(vcfFile, REQUIRE_INDEX_CFG);
    val workflowTypesSet = Sets.<WorkflowTypes>newHashSet();
    long nullCount = 0;
    for (val variant : vcf){
      val ssmPrimaryConsensus = buildSSMPrimary(sampleMetadataConsensus, variant);
      nullCount += countIllegalValues(ssmPrimaryConsensus);
      getSSMPrimaryTransformer(CONSENSUS).transform(ssmPrimaryConsensus);

      for (val workflowType : extractWorkflowTypes(variant) ){
        workflowTypesSet.add(workflowType);
        val ssmPrimary = createCallerSpecificSSMPrimary(sampleMetadataConsensus, ssmPrimaryConsensus, workflowType);
        nullCount += countIllegalValues(ssmPrimary);
        getSSMPrimaryTransformer(workflowType).transform(ssmPrimary);
      }
    }
    if (nullCount >0 ){
      log.error("The ssmPrimary file {} has {} null values", vcfFile.getName(), nullCount);
    }

    //Write SSM Metadata to file
    val ssmMetadataConsensus = newSSMMetadata(sampleMetadataConsensus);
    checkForIllegalMetadata(CONSENSUS,vcfFile.getName(), ssmMetadataConsensus);
    getSSMMetadataTransformer(CONSENSUS).transform(ssmMetadataConsensus);

    for (val workflowType : workflowTypesSet){
      val ssmMetadata = createCallerSpecificSSMMetadata(sampleMetadataConsensus,ssmMetadataConsensus,workflowType);
      checkForIllegalMetadata(workflowType,vcfFile.getName(), ssmMetadata);
      getSSMMetadataTransformer(workflowType).transform(ssmMetadata);
    }

    // Close transformers and release memory
    closeAllMetadataTransformers();
    closeAllPrimaryTransformers();


  }

  private void convertAndTransform(WorkflowTypes workflowType, SSMPrimary ssmPrimaryConsensus,  SampleMetadata sampleMetadataConsensus){
    getSSMPrimaryTransformer(workflowType)
        .transform(
            createCallerSpecificSSMPrimary(sampleMetadataConsensus, ssmPrimaryConsensus, workflowType));
  }

  private Transformer<SSMPrimary> buildSSMPrimaryTransformer(WorkflowTypes workflowType, String dccProjectCode){
    val primaryFWCtx = primaryFWCtxFactory.getFileWriterContext(workflowType, dccProjectCode);
    return primaryTransformerFactory.getTransformer(primaryFWCtx);
  }

  private Transformer<SSMMetadata> buildSSMMetadataTransformer(WorkflowTypes workflowType, String dccProjectCode){
    val metadataFWCtx = metadataFWCtxFactory.getFileWriterContext(workflowType, dccProjectCode);
    return metadataTransformerFactory.getTransformer(metadataFWCtx);
  }

  private void buildTransformerMaps(String dccProjectCode){
    primaryTransformerMap = newEnumMap(WorkflowTypes.class);
    metadataTransformerMap = newEnumMap(WorkflowTypes.class);
    for (val workflowType : WorkflowTypes.values()){
      primaryTransformerMap.put( workflowType, buildSSMPrimaryTransformer(workflowType, dccProjectCode) );
      metadataTransformerMap.put(workflowType, buildSSMMetadataTransformer(workflowType, dccProjectCode) );
    }
  }

  private Transformer<SSMPrimary> getSSMPrimaryTransformer(WorkflowTypes workflowType){
    checkArgument(primaryTransformerMap.containsKey(workflowType),
        "The primary Transformer map does not contain the workflowType [%s]", workflowType.getName());
    return primaryTransformerMap.get(workflowType);
  }

  private Transformer<SSMMetadata> getSSMMetadataTransformer(WorkflowTypes workflowType){
    checkArgument(metadataTransformerMap.containsKey(workflowType),
        "The metadata Transformer map does not contain the workflowType [%s]", workflowType.getName());
    return metadataTransformerMap.get(workflowType);
  }

  @SneakyThrows
  private void closeAllPrimaryTransformers() {
    for (val pt : primaryTransformerMap.values()){
      pt.close();
    }
    primaryTransformerMap = null;
  }

  @SneakyThrows
  private void closeAllMetadataTransformers() {
    for (val mt : metadataTransformerMap.values()){
      mt.close();
    }
    metadataTransformerMap = null;
  }

  private static SSMPrimary buildSSMPrimary(SampleMetadata sampleMetadata, VariantContext variant){
    val dataType = sampleMetadata.getDataType();
    val analysisId = sampleMetadata.getAnalysisId();
    val analyzedSampleId = sampleMetadata.getAnalyzedSampleId();

    if (dataType == INDEL){
      return newIndelSSMPrimary(variant, analysisId, analyzedSampleId);
    } else if(dataType == SNV_MNV){
      return newSnvMnvSSMPrimary(variant, analysisId, analyzedSampleId);
    } else {
      throw new IllegalStateException("The dataType "+dataType.getName()+" is unsupported or implemented");
    }
  }


  private static Set<WorkflowTypes> extractWorkflowTypes(VariantContext  variant){
    return streamCallers(variant)
        .map(c ->  WorkflowTypes.parseMatch(c, F_CHECK_CORRECT_WORKTYPE ))
        .collect(toImmutableSet());
  }

  // As specified in the spec, the callerSpecific ssmPrimary is the same as the consensus one,
  // with only the analysisId, totalReadCount and mutantAlleleReadCount being
  // different (hence the cloning and modifing)
  private static SSMPrimary createCallerSpecificSSMPrimary(SampleMetadata sampleMetadataConsensus,
      SSMPrimary ssmPrimaryConsensus, WorkflowTypes workflowType){
    val callerSampleMetadata = SampleMetadata.builderWith(sampleMetadataConsensus)
        .workflowType(workflowType) // Overwrite workflowType, since everything else is the same
        .build();
    return PlainSSMPrimary.builderWith(ssmPrimaryConsensus)
        .analysisId(callerSampleMetadata.getAnalysisId())
        .totalReadCount(DATA_VERIFIED_TO_BE_UNKNOWN.toInt())
        .mutantAlleleReadCount(DATA_VERIFIED_TO_BE_UNKNOWN.toInt())
        .build();
  }

  private static SSMMetadata createCallerSpecificSSMMetadata(SampleMetadata sampleMetadataConsensus,
      SSMMetadata ssmMetadataConsensus, WorkflowTypes workflowType){
    val callerSampleMetadata = SampleMetadata.builderWith(sampleMetadataConsensus)
        .workflowType(workflowType) // Overwrite workflowType, since everything else is the same
        .build();
    val variationCallingAlgorithmText = VariationCallingAlgorithms.get(workflowType, callerSampleMetadata.getDataType());
    return PlainSSMMetadata.builderWith(ssmMetadataConsensus)
        .analysisId(callerSampleMetadata.getAnalysisId())
        .variationCallingAlgorithm(variationCallingAlgorithmText)
        .build();
  }


}
