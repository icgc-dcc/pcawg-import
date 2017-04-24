package org.icgc.dcc.pcawg.client.vcf;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFFileReader;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.model.ssm.primary.SSMPrimaryErrors;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.joining;

public class VariantVerifier implements Iterable<VariantVerifier.VariantStatus> {

  private final Map<String, List<VariantStatus>> statuses = Maps.newHashMap();
  //TODO: added an output file, where the statuses will be writen to

  public void add(String filename, VCFFileReader vcfFileReader, VariantContext variantContext, long lineNumber){
    val status = VariantStatus.builder()
    .filename(filename)
    .lineNumber(lineNumber)
    .variantContext(variantContext)
    .vcfFileReader(vcfFileReader)
    .build();
    val hasErrors = status.checkForErrors();
    if (hasErrors){
      if (! statuses.containsKey(filename)){
        statuses.put(filename, Lists.newArrayList());
      }
      val list =statuses.get(filename);
      list.add(status);
    }
  }

  public List<VariantStatus> get(String filename){
    Preconditions.checkArgument(statuses.containsKey(filename), "The filename {} DNE", filename);
    return statuses.get(filename);
  }

  @Override
  public Iterator<VariantStatus> iterator() {
    return statuses.values().stream().flatMap(Collection::stream).iterator();
  }

  public void log(){
    for (val status : this){
      status.log();
    }
  }

  @Builder
  @Slf4j
  public static class VariantStatus{
    private static final String T_REF_COUNT = "t_ref_count";
    private static final String T_ALT_COUNT = "t_alt_count";

    @NonNull
    private final String filename;

    @NonNull
    private final VCFFileReader vcfFileReader;

    @NonNull
    private final VariantContext variantContext;

    private final long lineNumber;

    private Set<SSMPrimaryErrors> primaryErrors = Sets.newHashSet();

    private boolean checkForGeneralPrimaryErrors(){
      val info = variantContext.getCommonInfo();

      boolean hasErrors = false;
      if (!info.hasAttribute(T_REF_COUNT)){
        primaryErrors.add(SSMPrimaryErrors.T_REF_COUNT_ATTRIBUTE_DOES_NOT_EXIST_ERROR);
        hasErrors = true;
      }
      if (!info.hasAttribute(T_ALT_COUNT)){
        primaryErrors.add(SSMPrimaryErrors.T_ALT_COUNT_ATTRIBUTE_DOES_NOT_EXIST_ERROR);
        hasErrors = true;
      }
      return hasErrors;
    }

    private boolean checkForIndelErrors(){
      val refAlleleLength =  variantContext.getReference().length();
      val altAlleleLength = variantContext.getAlternateAllele(0).length();
      if (refAlleleLength == altAlleleLength){
        primaryErrors.add(SSMPrimaryErrors.INDEL_IDENTICAL_REF_AND_ALT_LENGTH_ERROR);
        return true;
      }
      return false;
    }

    public boolean checkForErrors(){
      // For other checks, should be ORing the result, and the return of this gives the final error result
      boolean hasGeneralPrimaryErrors = false;
      hasGeneralPrimaryErrors |= checkForGeneralPrimaryErrors();
      hasGeneralPrimaryErrors |= checkForIndelErrors();
      return hasGeneralPrimaryErrors;
    }

    public void log(){
      log.info("Filename,{},LineNumber,{},VariantContext,{},PrimaryErrors,{}",
          filename,
          lineNumber,
          variantContext.toString(),
          primaryErrors.stream().map(Enum::name).collect(joining(";"))
      );
    }
  }




}
