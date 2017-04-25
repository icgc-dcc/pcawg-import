package org.icgc.dcc.pcawg.client.vcf.converters.variant;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.model.ssm.primary.SSMPrimary;
import org.icgc.dcc.pcawg.client.core.types.DataTypes;
import org.icgc.dcc.pcawg.client.core.types.MutationTypes;
import org.icgc.dcc.pcawg.client.vcf.DataTypeConversionException;
import org.icgc.dcc.pcawg.client.vcf.VCF;
import org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantException;

import java.util.Set;

import static org.icgc.dcc.pcawg.client.core.types.DataTypes.INDEL;
import static org.icgc.dcc.pcawg.client.core.types.DataTypes.SNV_MNV;
import static org.icgc.dcc.pcawg.client.core.types.MutationTypes.DELETION_LTE_200BP;
import static org.icgc.dcc.pcawg.client.core.types.MutationTypes.INSERTION_LTE_200BP;
import static org.icgc.dcc.pcawg.client.core.types.MutationTypes.MULTIPLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.pcawg.client.core.types.MutationTypes.SINGLE_BASE_SUBSTITUTION;
import static org.icgc.dcc.pcawg.client.core.types.MutationTypes.UNKNOWN;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getFirstAlternativeAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.VCF.getReferenceAlleleLength;
import static org.icgc.dcc.pcawg.client.vcf.errors.PcawgVariantErrors.MUTATION_TYPE_NOT_SUPPORTED_ERROR;

public interface VariantProcessor {
  boolean DEFAULT_THROW_EXCEPTION_FLAG = true;

  static MutationTypes resolveMutationType(VariantContext v){
    return resolveMutationType(DEFAULT_THROW_EXCEPTION_FLAG, v);
  }

  static MutationTypes resolveMutationType(boolean throwException, VariantContext v){
    val ref = VCF.getReferenceAlleleString(v);
    val alt = VCF.getFirstAlternativeAlleleString(v);
    val refLength = getReferenceAlleleLength(v);
    val altLength = getFirstAlternativeAlleleLength(v);
    val altIsOne = altLength ==1;
    val refIsOne = refLength ==1;
    val refStartsWithAlt = ref.startsWith(alt);
    val altStartsWithRef = alt.startsWith(ref);
    val  lengthDifference = refLength - altLength;

    if (lengthDifference < 0 && !altIsOne && !refStartsWithAlt){
      return refIsOne && altStartsWithRef ? INSERTION_LTE_200BP : MULTIPLE_BASE_SUBSTITUTION;

    } else if(lengthDifference == 0 && !refStartsWithAlt && !altStartsWithRef){
      return refIsOne ? SINGLE_BASE_SUBSTITUTION : MULTIPLE_BASE_SUBSTITUTION;

    } else if(lengthDifference > 0 && !refIsOne && !altStartsWithRef ){
      return altIsOne && refStartsWithAlt ? DELETION_LTE_200BP : MULTIPLE_BASE_SUBSTITUTION;

    } else {
      val message = String.format("The mutationType of the variant cannot be resolved: Ref[%s] Alt[%s] --> RefLength-AltLength=%s,  RefLengthIsOne[%s] AltLengthIsOne[%s], RefStartsWithAlt[%s] AltStartsWithRef[%s] ", lengthDifference, ref, alt, refIsOne, altIsOne, refStartsWithAlt, altStartsWithRef);

      if (throwException){
        throw new PcawgVariantException(message, v, MUTATION_TYPE_NOT_SUPPORTED_ERROR);
      } else {
        return UNKNOWN;
      }
    }
  }

  static DataTypes resolveDataType(VariantContext variantContext){
    val mutationType = resolveMutationType(variantContext);
    return resolveDataType(mutationType);
  }

  static DataTypes resolveDataType(MutationTypes mutationType){
    if (mutationType == SINGLE_BASE_SUBSTITUTION || mutationType == MULTIPLE_BASE_SUBSTITUTION){
      return SNV_MNV;
    } else if (mutationType == DELETION_LTE_200BP || mutationType == INSERTION_LTE_200BP){
      return INDEL;
    } else if (mutationType == UNKNOWN) {
      return DataTypes.UNKNOWN;
    } else {
      throw new DataTypeConversionException(String.format("No implementation defined for converting the MutationType [%s] to a DataType", mutationType.name()));
    }
  }

  Set<SSMPrimary> convertSSMPrimary(VariantContext variant);

}
