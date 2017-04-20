package org.icgc.dcc.pcawg.client.download;

import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import htsjdk.variant.vcf.VCFFileReader;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.assertj.core.api.Assertions;
import org.icgc.dcc.common.core.util.stream.Collectors;
import org.icgc.dcc.common.core.util.stream.Streams;
import org.icgc.dcc.pcawg.client.core.Factory;
import org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata.SampleMetadataBuilder;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.SSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.metadata.impl.PcawgSSMMetadata;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimaryFieldMapping;
import org.icgc.dcc.pcawg.client.model.ssm.primary.impl.PlainSSMPrimary;
import org.icgc.dcc.pcawg.client.utils.SetLogic;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.SSMPrimaryClassification;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;

import static com.google.common.collect.Lists.newArrayList;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.core.util.Joiners.UNDERSCORE;
import static org.icgc.dcc.pcawg.client.core.transformer.impl.DccTransformerContext.newDccTransformerContext;
import static org.icgc.dcc.pcawg.client.model.ssm.primary.impl.IndelPcawgSSMPrimary.newIndelSSMPrimary;
import static org.icgc.dcc.pcawg.client.model.ssm.primary.impl.SnvMnvPcawgSSMPrimary.newSnvMnvSSMPrimary;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.DELETION_LTE_200BP;
import static org.icgc.dcc.pcawg.client.vcf.MutationTypes.INSERTION_LTE_200BP;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.BROAD;
import static org.icgc.dcc.pcawg.client.vcf.WorkflowTypes.CONSENSUS;

@Slf4j
public class SSMTest {

  // process vcf and iterate through each variantContext element
  private static final boolean REQUIRE_INDEX_CFG = false;
  private static final String INDEL_FIXTURE_FILENAME = "f9c4e06c-e8a6-613b-e040-11ac0d4828ba.consensus.20160830.somatic.indel.vcf.gz";
  private static final String INDEL_INSERTION_VCF_FILENAME = "fixtures/test_indel_insertion.vcf";
  private static final String INDEL_DELETION_VCF_FILENAME = "fixtures/test_indel_deletion.vcf";
  private static final String SNV_MNV_SINGLE_BASE_VCF_FILENAME = "fixtures/test_snv_mnv_single_base.vcf";
  private static final String SNV_MNV_MULTIPLE_BASE_VCF_FILENAME = "fixtures/test_snv_mnv_multiple_base.vcf";

  private static final String INSERTION_MUTATION_TYPE = "insertion of <=200bp";
  private static final String DELETION_MUTATION_TYPE = "deletion of <=200bp";
  private static final String SINGLE_BASE_SUBSTITUTION_MUTATION_TYPE = "single base substitution";
  private static final String MULTIPLE_BASE_SUBSTITUTION_MUTATION_TYPE =
      "multiple base substitution (>=2bp and <=200bp)";

  private static final WorkflowTypes FIXED_CONSENSUS_WORKFLOW = CONSENSUS;
  private static final DataTypes FIXED_INDEL_DATATYPE = DataTypes.INDEL;
  private static final DataTypes DUMMY_DATA_TYPE = DataTypes.INDEL;
  private static final String DUMMY_ANALYSIS_ID = UNDERSCORE.join("myDccProjectCode",
      FIXED_CONSENSUS_WORKFLOW.getName(), FIXED_INDEL_DATATYPE.getName());
  private static final String DUMMY_ANALYZED_SAMPLE_ID = "myAnalyzedSampleId";
  private static final String DUMMY_MATCHED_SAMPLE_ID = "myMatchedSampleId";

  private static final String DEFAULT_ASSEMBLY_VERSION = "GRCh37";
  private static final String DEFAULT_PLATFORM = "Illumina HiSeq";
  private static final String DEFAULT_VARIATION_CALLING_ALGORITHM= "consensus";
  private static final String NA_VALUE = "-777";
  private static final String WGS = "WGS";
  private static final String EGA = "EGA";
  private static final String TCGA = "TCGA";


  private static final SampleMetadataBuilder TEMPLATE_SAMPLE_METADATA_BUILDER = SampleMetadata.builder()
      .analyzedSampleId(DUMMY_ANALYZED_SAMPLE_ID)
      .matchedSampleId(DUMMY_MATCHED_SAMPLE_ID)
      .aliquotId("myAliquotId")
      .dccProjectCode("myDccProjectCode")
      .analyzedFileId("myAnalyzedFileId")
      .matchedFileId("myMatchedFileId")
      .workflowType(FIXED_CONSENSUS_WORKFLOW);

  private static final SampleMetadata DUMMY_NON_US_SAMPLE_METADATA = TEMPLATE_SAMPLE_METADATA_BUILDER
      .isUsProject(false)
      .build();

  private static final SampleMetadata DUMMY_US_SAMPLE_METADATA = TEMPLATE_SAMPLE_METADATA_BUILDER
      .isUsProject(true)
      .build();

  private static SSMPrimary getFirstSSMIndelPrimary(String vcfFilename) {
    val vcf = readVCF(vcfFilename);
    SSMPrimary ssmPrimary = null;
    for (val variant : vcf) {
      ssmPrimary = newIndelSSMPrimary(
          variant,
          DUMMY_ANALYSIS_ID,
          DUMMY_ANALYZED_SAMPLE_ID);
      break; //only first line
    }
    return ssmPrimary;
  }

  private static SSMPrimary getFirstSSMSnvMnvPrimary(String vcfFilename) {
    val vcf = readVCF(vcfFilename);
    SSMPrimary ssmPrimary = null;
    for (val variant : vcf) {
      ssmPrimary = newSnvMnvSSMPrimary(variant, DUMMY_ANALYSIS_ID, DUMMY_ANALYZED_SAMPLE_ID);
      break; //only first line
    }
    return ssmPrimary;
  }

  private static void assertSSMPrimary(SSMPrimary exp, SSMPrimary act) {
    assertThat(exp.getAnalysisId()).isEqualTo(act.getAnalysisId());
    assertThat(exp.getAnalyzedSampleId()).isEqualTo(act.getAnalyzedSampleId());
    assertThat(exp.getPcawgFlag()).isEqualTo(act.getPcawgFlag());
    assertThat(exp.getMutationType()).isEqualTo(act.getMutationType());
    assertThat(exp.getChromosome()).isEqualTo(act.getChromosome());
    assertThat(exp.getChromosomeStart()).isEqualTo(act.getChromosomeStart());
    assertThat(exp.getChromosomeEnd()).isEqualTo(act.getChromosomeEnd());
    assertThat(exp.getChromosomeStrand()).isEqualTo(act.getChromosomeStrand());
    assertThat(exp.getReferenceGenomeAllele()).isEqualTo(act.getReferenceGenomeAllele());
    assertThat(exp.getControlGenotype()).isEqualTo(act.getControlGenotype());
    assertThat(exp.getMutatedFromAllele()).isEqualTo(act.getMutatedFromAllele());
    assertThat(exp.getTumorGenotype()).isEqualTo(act.getTumorGenotype());
    assertThat(exp.getMutatedToAllele()).isEqualTo(act.getMutatedToAllele());
    assertThat(exp.getExpressedAllele()).isEqualTo(act.getExpressedAllele());
    assertThat(exp.getQualityScore()).isEqualTo(act.getQualityScore());
    assertThat(exp.getProbability()).isEqualTo(act.getProbability());
    assertThat(exp.getTotalReadCount()).isEqualTo(act.getTotalReadCount());
    assertThat(exp.getMutantAlleleReadCount()).isEqualTo(act.getMutantAlleleReadCount());
    assertThat(exp.getVerificationStatus()).isEqualTo(act.getVerificationStatus());
    assertThat(exp.getVerificationPlatform()).isEqualTo(act.getVerificationPlatform());
    assertThat(exp.getBiologicalValidationStatus()).isEqualTo(act.getBiologicalValidationStatus());
    assertThat(exp.getBiologicalValidationPlatform()).isEqualTo(act.getBiologicalValidationPlatform());
    assertThat(exp.getNote()).isEqualTo(act.getNote());
  }

  private static SSMPrimary createDeletion(String chromosome, int pos, String ref, String alt, int tRefCount,
      int tAltCount) {
    val refLength = ref.length();
    val altLength = alt.length();
    val refFirstUpstreamRemoved = ref.substring(1);
    assertThat(refLength - 1).isEqualTo(refFirstUpstreamRemoved.length());
    return PlainSSMPrimary.builder()
        .analysisId(DUMMY_ANALYSIS_ID)
        .analyzedSampleId(DUMMY_ANALYZED_SAMPLE_ID)
        .mutationType(DELETION_MUTATION_TYPE)
        .chromosome(chromosome)
        .chromosomeStart(pos + 1)
        .chromosomeEnd(pos + refLength - 1)
        .chromosomeStrand(1)
        .referenceGenomeAllele(refFirstUpstreamRemoved)
        .controlGenotype(refFirstUpstreamRemoved + "/" + refFirstUpstreamRemoved)
        .mutatedFromAllele(refFirstUpstreamRemoved)
        .tumorGenotype(refFirstUpstreamRemoved + "/-")
        .mutatedToAllele("-")
        .expressedAllele("-777")
        .qualityScore("-777")
        .probability("-777")
        .totalReadCount(tAltCount + tRefCount)
        .mutantAlleleReadCount(tAltCount)
        .verificationStatus("not tested")
        .verificationPlatform("-777")
        .biologicalValidationStatus("-777")
        .biologicalValidationPlatform("-777")
        .note("-777")
        .pcawgFlag(true)
        .build();
  }

  private static SSMPrimary createInsertion(String chromosome, int pos, String ref, String alt, int tRefCount,
      int tAltCount) {
    val refLength = ref.length();
    val altLength = alt.length();
    val refFirstUpstreamRemoved = ref.substring(1);
    val altFirstUpstreamRemoved = alt.substring(1);
    assertThat(refLength - 1).isEqualTo(refFirstUpstreamRemoved.length());
    assertThat(altLength - 1).isEqualTo(altFirstUpstreamRemoved.length());
    return PlainSSMPrimary.builder()
        .analysisId(DUMMY_ANALYSIS_ID)
        .analyzedSampleId(DUMMY_ANALYZED_SAMPLE_ID)
        .mutationType(INSERTION_MUTATION_TYPE)
        .chromosome(chromosome)
        .chromosomeStart(pos + 1)
        .chromosomeEnd(pos + 1)
        .chromosomeStrand(1)
        .referenceGenomeAllele("-")
        .controlGenotype("-/-")
        .mutatedFromAllele("-")
        .tumorGenotype("-/" + altFirstUpstreamRemoved)
        .mutatedToAllele(altFirstUpstreamRemoved)
        .expressedAllele("-777")
        .qualityScore("-777")
        .probability("-777")
        .totalReadCount(tAltCount + tRefCount)
        .mutantAlleleReadCount(tAltCount)
        .verificationStatus("not tested")
        .verificationPlatform("-777")
        .biologicalValidationStatus("-777")
        .biologicalValidationPlatform("-777")
        .note("-777")
        .pcawgFlag(true)
        .build();
  }

  private static SSMPrimary createSingleBase(String chromosome, int pos, String ref, String alt, int tRefCount,
      int tAltCount) {
    val refLength = ref.length();
    return PlainSSMPrimary.builder()
        .analysisId(DUMMY_ANALYSIS_ID)
        .analyzedSampleId(DUMMY_ANALYZED_SAMPLE_ID)
        .mutationType(SINGLE_BASE_SUBSTITUTION_MUTATION_TYPE)
        .chromosome(chromosome)
        .chromosomeStart(pos)
        .chromosomeEnd(pos + refLength - 1)
        .chromosomeStrand(1)
        .referenceGenomeAllele(ref)
        .controlGenotype(ref + "/" + ref)
        .mutatedFromAllele(ref)
        .tumorGenotype(ref + "/" + alt)
        .mutatedToAllele(alt)
        .expressedAllele("-777")
        .qualityScore("-777")
        .probability("-777")
        .totalReadCount(tAltCount + tRefCount)
        .mutantAlleleReadCount(tAltCount)
        .verificationStatus("not tested")
        .verificationPlatform("-777")
        .biologicalValidationStatus("-777")
        .biologicalValidationPlatform("-777")
        .note("-777")
        .pcawgFlag(true)
        .build();
  }

  private static SSMPrimary createMultipleBase(String chromosome, int pos, String ref, String alt, int tRefCount,
      int tAltCount) {
    val refLength = ref.length();
    return PlainSSMPrimary.builder()
        .analysisId(DUMMY_ANALYSIS_ID)
        .analyzedSampleId(DUMMY_ANALYZED_SAMPLE_ID)
        .mutationType(MULTIPLE_BASE_SUBSTITUTION_MUTATION_TYPE)
        .chromosome(chromosome)
        .chromosomeStart(pos)
        .chromosomeEnd(pos + refLength - 1)
        .chromosomeStrand(1)
        .referenceGenomeAllele(ref)
        .controlGenotype(ref + "/" + ref)
        .mutatedFromAllele(ref)
        .tumorGenotype(ref + "/" + alt)
        .mutatedToAllele(alt)
        .expressedAllele("-777")
        .qualityScore("-777")
        .probability("-777")
        .totalReadCount(tAltCount + tRefCount)
        .mutantAlleleReadCount(tAltCount)
        .verificationStatus("not tested")
        .verificationPlatform("-777")
        .biologicalValidationStatus("-777")
        .biologicalValidationPlatform("-777")
        .note("-777")
        .pcawgFlag(true)
        .build();
  }

  @SneakyThrows
  private static VCFFileReader readVCF(String filename) {
    val url = Resources.getResource(filename);
    val file = new File(url.toURI());
    return new VCFFileReader(file, REQUIRE_INDEX_CFG);
  }

  private void assertCommonSMMMetadata(SSMMetadata ssmMetadata){
    assertThat(ssmMetadata.getAnalysisId()).isEqualTo(DUMMY_ANALYSIS_ID);
    assertThat(ssmMetadata.getAnalyzedSampleId()).isEqualTo(DUMMY_ANALYZED_SAMPLE_ID);
    assertThat(ssmMetadata.getMatchedSampleId()).isEqualTo(DUMMY_MATCHED_SAMPLE_ID);
    assertThat(ssmMetadata.getAssemblyVersion()).isEqualTo(DEFAULT_ASSEMBLY_VERSION);
    assertThat(ssmMetadata.getPlatform()).isEqualTo(DEFAULT_PLATFORM);
    assertThat(ssmMetadata.getExperimentalProtocol()).isEqualTo(NA_VALUE);
    assertThat(ssmMetadata.getBaseCallingAlgorithm()).isEqualTo(NA_VALUE);
    assertThat(ssmMetadata.getAlignmentAlgorithm()).isEqualTo(NA_VALUE);
    assertThat(ssmMetadata.getVariationCallingAlgorithm()).isEqualTo(DEFAULT_VARIATION_CALLING_ALGORITHM);
    assertThat(ssmMetadata.getOtherAnalysisAlgorithm()).isEqualTo(NA_VALUE);
    assertThat(ssmMetadata.getSequencingStrategy()).isEqualTo(WGS);
    assertThat(ssmMetadata.getSeqCoverage()).isEqualTo(NA_VALUE);
  }

  private static String calcAnalysisId(String dccProjectCode, WorkflowTypes workflowType, DataTypes dataType){
    return UNDERSCORE.join(dccProjectCode, workflowType.getName(), dataType.getName());
  }

  private static final SSMMetadata createSSMMetadata(SampleMetadata sampleMetadata){
    val analysisId = calcAnalysisId(sampleMetadata.getDccProjectCode(), sampleMetadata.getWorkflowType(), DUMMY_DATA_TYPE);
    return PcawgSSMMetadata.newSSMMetadataImpl(
        sampleMetadata.getWorkflowType().getName(),
        sampleMetadata.getMatchedSampleId(),
        analysisId,
        sampleMetadata.getAnalyzedSampleId(),
        sampleMetadata.isUsProject(),
        sampleMetadata.getAliquotId(),
        sampleMetadata.getAnalyzedFileId(),
        sampleMetadata.getMatchedFileId());
  }


  @Test
  public void testIndelInsertion() {
    val ssmIndelPrimaryActual = getFirstSSMIndelPrimary(INDEL_INSERTION_VCF_FILENAME);
    val pos = 2277483;
    val ref = "A";
    val alt = "ATG";
    val chromosome = "1";
    val t_alt_count = 4;
    val t_ref_count = 44;

    val ssmIndelPrimaryExpected = createInsertion(chromosome, pos, ref, alt, t_ref_count, t_alt_count);
    assertThat(ssmIndelPrimaryActual).isNotNull();
    assertSSMPrimary(ssmIndelPrimaryActual, ssmIndelPrimaryExpected);
  }

  @Test
  public void testIndelDeletion() {
    val ssmIndelPrimaryActual = getFirstSSMIndelPrimary(INDEL_DELETION_VCF_FILENAME);
    val pos = 2897557;
    val ref = "CAACTTATATATT";
    val alt = "C";
    val chromosome = "1";
    val t_alt_count = 1;
    val t_ref_count = 52;
    val ssmIndelPrimaryExpected = createDeletion(chromosome, pos, ref, alt, t_ref_count, t_alt_count);
    assertThat(ssmIndelPrimaryActual).isNotNull();
    assertSSMPrimary(ssmIndelPrimaryActual, ssmIndelPrimaryExpected);
  }

  @Test
  public void testSnvMnvSingleBase() {
    val ssmSnvMnvPrimaryActual = getFirstSSMSnvMnvPrimary(SNV_MNV_SINGLE_BASE_VCF_FILENAME);
    val pos = 2897557;
    val ref = "A";
    val alt = "C";
    val chromosome = "1";
    val t_alt_count = 1;
    val t_ref_count = 52;
    val ssmSnvMnvPrimaryExpected = createSingleBase(chromosome, pos, ref, alt, t_ref_count, t_alt_count);
    assertThat(ssmSnvMnvPrimaryActual).isNotNull();
    assertSSMPrimary(ssmSnvMnvPrimaryActual, ssmSnvMnvPrimaryExpected);
  }

  @Test
  public void testSnvMnvMultipleBase() {
    val ssmSnvMnvPrimaryActual = getFirstSSMSnvMnvPrimary(SNV_MNV_MULTIPLE_BASE_VCF_FILENAME);
    val pos = 2897557;
    val ref = "ATG";
    val alt = "AGT";
    val chromosome = "1";
    val t_alt_count = 1;
    val t_ref_count = 52;
    val ssmSnvMnvPrimaryExpected = createMultipleBase(chromosome, pos, ref, alt, t_ref_count, t_alt_count);
    assertThat(ssmSnvMnvPrimaryActual).isNotNull();
    assertSSMPrimary(ssmSnvMnvPrimaryActual, ssmSnvMnvPrimaryExpected);
  }

  @Test
  public void testUsSSMMetadata() {
    val ssmMetadata = createSSMMetadata(DUMMY_US_SAMPLE_METADATA);
    assertCommonSMMMetadata(ssmMetadata);
    assertThat(ssmMetadata.getRawDataRepository()).isEqualTo(TCGA);
    assertThat(ssmMetadata.getRawDataAccession()).isEqualTo(DUMMY_US_SAMPLE_METADATA.getAnalyzedSampleId());
  }

  /**
   * TODO: [DCC-5507] once DCC-5507 is complete, need to implement this properly, fix this test and unIgnore it
   */
  @Test
  @Ignore("getRawDataAccession() not properly implemented yet")
  public void testNonUsSSMMetadata() {
    val ssmMetadata = createSSMMetadata(DUMMY_NON_US_SAMPLE_METADATA);
    assertCommonSMMMetadata(ssmMetadata);
    assertThat(ssmMetadata.getRawDataRepository()).isEqualTo(EGA);
    Assertions.fail("getRawDataAccession() not properly implemented yet");
  }

  @Test
  public void testFieldNames(){
    val dictionaryCreator = Factory.buildDictionaryCreator();
    val schema = dictionaryCreator.getSSMPrimaryFileSchema();
    val expectedSet = Sets.newHashSet(schema.fieldNames());

    // This is a new field that is needed for Andy
    expectedSet.add(SSMPrimaryFieldMapping.PCAWG_FLAG.toString());

    val actualSet = Streams.stream(SSMPrimaryFieldMapping.values())
        .map(SSMPrimaryFieldMapping::toString)
        .collect(Collectors.toImmutableSet());
    val missingFromActual = SetLogic.missingFromActual(actualSet, expectedSet);
    val extraInActual = SetLogic.extraInActual(actualSet,expectedSet);
    boolean result = true;
    if (!missingFromActual.isEmpty()){
      log.error("The following is missing from the [{}] field list: \n{}",
          SSMPrimaryFieldMapping.class.getName(), missingFromActual.stream().collect(joining("\n")));
      result = false;
    }

    if (!extraInActual.isEmpty()){
      log.error("The following are extra fields in the [{}] field list: \n{}",
          SSMPrimaryFieldMapping.class.getName(), extraInActual.stream().collect(joining("\n")));
      result = false;
    }

    assertThat(result).isTrue();
  }

  @Test
  public void testFieldOrder(){
    val dictionaryCreator = Factory.buildDictionaryCreator();
    val schema = dictionaryCreator.getSSMPrimaryFileSchema();
    val actualArray = SSMPrimaryFieldMapping.values();
    val expectedList = newArrayList(schema.fieldNames());
    expectedList.add(SSMPrimaryFieldMapping.PCAWG_FLAG.toString()); //This field is extra and appended to the end

    val actualSize =actualArray.length;
    val expectedSize = expectedList.size() ;

    assertThat(actualSize).isEqualTo(expectedSize);

    boolean  result = true;
    for (int i = 0; i < expectedSize; i++){
      val actual = actualArray[i].toString();
      val expected = expectedList.get(i);
      if (!expected.equals(actual)){
        log.error("Expected: [{}]   Actual: [{}]", expected, actual);
        result = false;
      }
    }
    assertThat(result).isTrue();
  }

  @Test
  public void testUniqueSSMClassification(){
    val sc1 = SSMPrimaryClassification.newSSMPrimaryClassification(CONSENSUS, INSERTION_LTE_200BP);
    val sc2 = SSMPrimaryClassification.newSSMPrimaryClassification(CONSENSUS, DELETION_LTE_200BP);
    val sc3 = SSMPrimaryClassification.newSSMPrimaryClassification(BROAD, DELETION_LTE_200BP);
    val sc1_clone = SSMPrimaryClassification.newSSMPrimaryClassification(CONSENSUS, INSERTION_LTE_200BP);

    assertThat(sc1).isNotEqualTo(sc2);
    assertThat(sc1).isNotEqualTo(sc3);
    assertThat(sc2).isNotEqualTo(sc3);
    assertThat(sc1).isEqualTo(sc1_clone);

  }

  @Test
  public void testUniqueDccTransformerContextSSMMetadata(){
    val sc1 = SSMPrimaryClassification.newSSMPrimaryClassification(CONSENSUS, INSERTION_LTE_200BP);
    val sc2 = SSMPrimaryClassification.newSSMPrimaryClassification(CONSENSUS, DELETION_LTE_200BP);
    val sc3 = SSMPrimaryClassification.newSSMPrimaryClassification(BROAD, DELETION_LTE_200BP);
    val sc1_clone = SSMPrimaryClassification.newSSMPrimaryClassification(CONSENSUS, INSERTION_LTE_200BP);
    val sampleMetadata = TEMPLATE_SAMPLE_METADATA_BUILDER.analyzedFileId("f1").matchedFileId("f2").build();
    val ssmMetadata = createSSMMetadata(sampleMetadata);

    val dtc1 = newDccTransformerContext(sc1, ssmMetadata);
    val dtc1_clone = newDccTransformerContext(sc1_clone, ssmMetadata);
    val dtc2 = newDccTransformerContext(sc2, ssmMetadata);
    val dtc3 = newDccTransformerContext(sc3, ssmMetadata);

    assertThat(dtc1).isNotEqualTo(dtc2);
    assertThat(dtc1).isNotEqualTo(dtc3);
    assertThat(dtc2).isNotEqualTo(dtc3);
    assertThat(dtc1).isEqualTo(dtc1_clone);
    assertThat(dtc1 == dtc1_clone).isFalse();

    val set = Sets.<DccTransformerContext<SSMMetadata>>newHashSet();
    set.add(dtc1);
    set.add(dtc1_clone);
    assertThat(set).hasSize(1);
    assertThat(set.contains(dtc1)).isTrue();
    assertThat(set.contains(dtc1_clone)).isTrue();


  }



}
