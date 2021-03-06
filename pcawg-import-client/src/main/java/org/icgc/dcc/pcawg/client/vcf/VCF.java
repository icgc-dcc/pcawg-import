package org.icgc.dcc.pcawg.client.vcf;

import com.google.common.base.Joiner;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.CommonInfo;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFEncoder;
import htsjdk.variant.vcf.VCFFileReader;
import htsjdk.variant.vcf.VCFHeader;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.common.core.util.Joiners;

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Slf4j
public class VCF {

  private static final String T_REF_COUNT = "t_ref_count";
  private static final String T_ALT_COUNT = "t_alt_count";
  private static final String CALLERS = "Callers";
  private static final Joiner ALLELE_JOINER = Joiners.SLASH;
  private static final boolean REQUIRE_INDEX_CFG = false;
  private static final boolean ALLOW_MISSING_FIELDS_IN_HEADER_CFG = true;
  private static final boolean OUTPUT_TRAILING_FORMAT_FIELDS_CFG = true;

  public static VCFFileReader newDefaultVCFFileReader(File vcfFile){
    checkFileExists(vcfFile);
    return new VCFFileReader(vcfFile, REQUIRE_INDEX_CFG);
  }

  public static VCFEncoder newDefaultVCFEncoder(VCFFileReader vcfFileReader){
    return newDefaultVCFEncoder(vcfFileReader.getFileHeader());
  }

  public static VCFEncoder newDefaultVCFEncoder(VCFHeader vcfHeader){
    return new VCFEncoder(vcfHeader,ALLOW_MISSING_FIELDS_IN_HEADER_CFG,
        OUTPUT_TRAILING_FORMAT_FIELDS_CFG );
  }

  public static VCFEncoder newDefaultVCFEncoder(File file){
    val vcfFile = newDefaultVCFFileReader(file);
    return newDefaultVCFEncoder(vcfFile.getFileHeader());
  }

  public static Stream<VariantContext> newVariantStream(Path vcfPath){
    return newVariantStream(newDefaultVCFFileReader(vcfPath.toFile()));
  }
  public static Stream<VariantContext> newVariantStream(VCFFileReader vcfFileReader){
    return stream(vcfFileReader);
  }

  private static void checkFileExists(File vcfFile){
    checkArgument(vcfFile.exists(), "The VCF File [{}] DNE", vcfFile.getName());
  }

  public static CommonInfo getCommonInfo(VariantContext v){
    return v.getCommonInfo();
  }

  public static Map<String, Object> getInfoMap(VariantContext v){
    return getCommonInfo(v).getAttributes();
  }

  public static Optional<Integer> getIntAttribute(VariantContext v, String attr) {
    val info = getCommonInfo(v);
    if (!info.hasAttribute(attr)){
      return Optional.empty();
    }
    return Optional.of(info.getAttributeAsInt(attr, -1));
  }

  public static Optional<Integer> getAltCount(VariantContext v) {
    return getIntAttribute(v, T_ALT_COUNT);
  }

  public static Optional<Integer> getRefCount(VariantContext v) {
    return getIntAttribute(v,T_REF_COUNT);
  }

  public static Stream<String> streamCallers(VariantContext v) {
    if (! getCommonInfo(v).hasAttribute(CALLERS)){
      log.error("Variant does not have a [{}] attribute in info field: {}", CALLERS, v.toString());
    }
    return getCommonInfo(v)
        .getAttributeAsList(CALLERS)
        .stream()
        .map(Object::toString);
  }

  public static String getChomosome(VariantContext v){
    return v.getContig();
  }

  public static int getReferenceAlleleLength(VariantContext v){
    return getReferenceAllele(v).length();
  }

  public static String getReferenceAlleleString(VariantContext v){
    return getReferenceAllele(v).getBaseString();
  }

  public static Allele getReferenceAllele(VariantContext v){
    return v.getReference();
  }

  private static int getNumAlternativeAlleles(VariantContext v){
    return v.getAlternateAlleles().size();
  }

  /**
   * TODO: Assumption is there there is ONLY ONE alternative allele.
   * @throws IllegalStateException for when there is more than one alternative allele
   */
  public static Allele getFirstAlternativeAllele(VariantContext v){
    checkState(getNumAlternativeAlleles(v) == 1, "There is more than one alternative allele");
    return v.getAlternateAllele(0);
  }

  public static int getFirstAlternativeAlleleLength(VariantContext v){
    return getFirstAlternativeAllele(v).length();
  }

  public static String getFirstAlternativeAlleleString(VariantContext v) {
    return getFirstAlternativeAllele(v).getBaseString(); //get first alternative allele
  }

  public static int getStart(VariantContext v){
    return v.getStart();
  }

  public static int getEnd(VariantContext v){
    return v.getEnd();
  }

  public static String removeFirstBase(Allele a){
    return a.getBaseString().substring(1);
  }

  public static String getStrippedReferenceAlleleString(VariantContext v){
    return removeFirstBase(v.getReference());
  }

  public static String getStrippedFirstAlternativeAlleleString(VariantContext v){
    return removeFirstBase(getFirstAlternativeAllele(v));
  }

  public static String joinAlleles(String ref, String alt){
    return ALLELE_JOINER.join(ref, alt);
  }

}
