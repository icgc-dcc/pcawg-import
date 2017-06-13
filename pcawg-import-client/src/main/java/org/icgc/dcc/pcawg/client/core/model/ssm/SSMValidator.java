package org.icgc.dcc.pcawg.client.core.model.ssm;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import lombok.NonNull;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.model.ssm.primary.FieldExtractor;
import org.icgc.dcc.pcawg.client.core.types.NACodeTypes;
import org.icgc.dcc.submission.dictionary.model.FileSchema;
import org.icgc.dcc.submission.dictionary.model.Restriction;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableSet;
import static org.icgc.dcc.common.core.util.stream.Streams.stream;
import static org.icgc.dcc.pcawg.client.utils.SetLogic.extraInActual;
import static org.icgc.dcc.pcawg.client.utils.SetLogic.missingFromActual;
import static org.icgc.dcc.submission.dictionary.model.RestrictionType.REGEX;
import static org.icgc.dcc.submission.dictionary.model.RestrictionType.REQUIRED;

public final class SSMValidator <T, F extends org.icgc.dcc.pcawg.client.core.model.ssm.primary.FieldExtractor<T>> {

  protected static final Set<String> NA_CODE_SET = stream(NACodeTypes.values())
      .map(NACodeTypes::toString)
      .collect(toImmutableSet());

  public static final <T, F extends FieldExtractor<T>> SSMValidator<T, F> newSSMValidator(FileSchema schema, F[] fieldArray){
    return new SSMValidator<T, F>(schema, fieldArray);
  }

  private final FileSchema schema;
  private final F[] fieldArray;

  private SSMValidator(FileSchema schema, F[] fieldArray){
    this.schema = schema;
    this.fieldArray = fieldArray;
  }

  private boolean validateRegex(Restriction restriction, @NonNull String valueToValidate){
    if (restriction.getType() == REGEX){
      val patternString = restriction.getConfig().get("pattern").toString();
      val pattern = Pattern.compile(patternString);
      return pattern.matcher(valueToValidate).matches();
    } else {
      return true;
    }
  }

  private boolean validateRequired(Restriction restriction, String valueToValidate, boolean regexIsOk){
    if (restriction.getType() == REQUIRED){
      val acceptMissingCode  = restriction.getConfig().getBoolean("acceptMissingCode", false);
      if(acceptMissingCode && !regexIsOk){
        return NA_CODE_SET.contains(valueToValidate);
      } else{
        return regexIsOk;
      }
    } else {
      return true;
    }
  }

  public List<F> validateFields(T data){
    // check that regex is met, and so is requires and and if acceptMissingCode allowed
    val schemaFields = schema.getFields();

    val badList = ImmutableList.<F>builder();

    for (int i =0; i< schemaFields.size(); i++){
      val schemaField = schemaFields.get(i);
      val fieldMapping = fieldArray[i];
      val valueToValidate = fieldMapping.extractStringValue(data);
      boolean isOk = true;
      for (val restriction : schemaField.getRestrictions()){
        val regexResult = validateRegex(restriction, valueToValidate);
        isOk &= validateRequired(restriction, valueToValidate, regexResult);
      }
      if (!isOk){
        badList.add(fieldMapping);
      }
    }
    return badList.build();
  }

  public static  Set<SSMCommon> differenceOfMetadataAndPrimary(Iterable<? extends SSMCommon> ssmMetadatas, Iterable<? extends SSMCommon> ssmPrimarys){
    val mSet = Sets.<SSMCommon>newHashSet(ssmMetadatas);
    val pSet = Sets.<SSMCommon>newHashSet(ssmMetadatas);
    val outSet = ImmutableSet.<SSMCommon>builder();
    val missingFromMset = missingFromActual(mSet, pSet);
    val extraInMset = extraInActual(mSet, pSet);
    outSet.addAll(missingFromMset);
    outSet.addAll(extraInMset);
    return outSet.build();
  }
}
