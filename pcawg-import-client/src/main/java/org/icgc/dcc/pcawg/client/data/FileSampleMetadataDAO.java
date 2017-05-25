package org.icgc.dcc.pcawg.client.data;

import com.google.common.base.Function;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.model.metadata.file.PortalFilename;
import org.icgc.dcc.pcawg.client.model.metadata.project.SampleMetadata;
import org.icgc.dcc.pcawg.client.model.metadata.project.SampleSheetModel;
import org.icgc.dcc.pcawg.client.model.metadata.project.Uuid2BarcodeSheetModel;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.pcawg.client.data.SampleMetadataDAO.isUSProject;

@Slf4j
public class FileSampleMetadataDAO implements SampleMetadataDAO {

  private static final String WGS = "WGS";
  private static final String NORMAL = "normal";
  private static final boolean F_CHECK_CORRECT_WORKTYPE = true;
  private static final boolean F_CHECK_CORRECT_DATATYPE = true;

  public static FileSampleMetadataDAO newFileSampleMetadataDAO(String sampleSheetFilename, boolean sampleSheetHasHeader,
      String uuid2BarcodeSheetFilename, boolean uuid2BarcodeSheetHasHeader){
    return new FileSampleMetadataDAO(sampleSheetFilename, sampleSheetHasHeader,uuid2BarcodeSheetFilename, uuid2BarcodeSheetHasHeader);
  }

  @SneakyThrows
  private static <T> List<T> readTsv(@NonNull  String filename,
      final boolean hasHeader,
      @NonNull Function<String, T > lineConversionFunctor){
    val file = new File(filename);
    checkState(file.exists(), "File %s DNE", filename);
    val br = new BufferedReader(new FileReader(file));
    val skipValue = hasHeader ? 1 : 0;
    val list = br.lines()
        .skip(skipValue)
        .map(lineConversionFunctor::apply)
        .collect(toImmutableList());
    br.close();
    return list;
  }

  @NonNull
  private final String sampleSheetFilename;

  @Getter
  private final boolean sampleSheetHasHeader;

  @NonNull
  private final String uuid2BarcodeSheetFilename;

  @Getter
  private final boolean uuid2BarcodeSheetHasHeader;


  private final List<SampleSheetModel> sampleSheetList;
  private final List<Uuid2BarcodeSheetModel> uuid2BarcodeSheetList;

  private FileSampleMetadataDAO(String sampleSheetFilename, boolean sampleSheetHasHeader,
      String uuid2BarcodeSheetFilename, boolean uuid2BarcodeSheetHasHeader) {
    this.sampleSheetFilename = sampleSheetFilename;
    this.uuid2BarcodeSheetFilename = uuid2BarcodeSheetFilename;
    this.sampleSheetHasHeader = sampleSheetHasHeader;
    this.uuid2BarcodeSheetHasHeader = uuid2BarcodeSheetHasHeader;
    this.sampleSheetList = readSampleSheet(sampleSheetHasHeader);
    this.uuid2BarcodeSheetList = readUuid2BarcodeSheet(uuid2BarcodeSheetHasHeader);
  }

  public int getSampleSheetSize(){
    return sampleSheetList.size();
  }

  public int getUUID2BarcodeSheetSize(){
    return uuid2BarcodeSheetList.size();
  }

  private SampleSheetModel getFirstSampleSheet(String aliquotId){
    val aliquotIdStream= sampleSheetList.stream()
        .filter(s -> s.getAliquotId().equals(aliquotId));

    val aliquotIdResult = aliquotIdStream.findFirst();
    checkState(aliquotIdResult.isPresent(), "Could not find first SampleSheet for aliquot_id [%s]", aliquotId);
    return aliquotIdResult.get();
  }

  private String getAnalyzedSampleId(boolean isUsProject, String submitterSampleId ){
    if (isUsProject){
      val result = createSubmitterSampleIdStream(submitterSampleId).findFirst();
      checkState(result.isPresent(),
        "Could not find SampleSheet with submitter_sample_id [%s]", submitterSampleId );
      return result.get().getTcgaBarcode();
    } else {
      return submitterSampleId;
    }
  }

  private String getMatchedSampleId(boolean isUsProject, String donorUniqueId ){
    val result = createDonorUniqueIdStream(donorUniqueId).findFirst();
    checkState(result.isPresent(), "Could not find SampleSheet for with donor_unique_id [%s] and library_strategy [%s] and dcc_speciment_type containing [%s]", donorUniqueId, WGS, NORMAL);
    val submitterSampleId = result.get().getSubmitterSampleId();
    return getAnalyzedSampleId(isUsProject, submitterSampleId);
  }

  @Override
  public SampleMetadata fetchSampleMetadata(PortalFilename portalFilename){
    val aliquotId = portalFilename.getAliquotId();
    val workflowType = WorkflowTypes.parseStartsWith(portalFilename.getWorkflow(), F_CHECK_CORRECT_WORKTYPE);
    val dataType = DataTypes.parseMatch(portalFilename.getDataType(), F_CHECK_CORRECT_DATATYPE);
    val sampleSheetByAliquotId = getFirstSampleSheet(aliquotId);
    val dccProjectCode = sampleSheetByAliquotId.getDccProjectCode();
    val submitterSampleId = sampleSheetByAliquotId.getSubmitterSampleId();
    val donorUniqueId = sampleSheetByAliquotId.getDonorUniqueId();
    val isUsProject =  isUSProject(dccProjectCode);

    val analyzedSampleId = getAnalyzedSampleId(isUsProject,submitterSampleId);
    val matchedSampleId = getMatchedSampleId(isUsProject,donorUniqueId);
    return SampleMetadata.builder()
        .analyzedSampleId(analyzedSampleId)
        .dccProjectCode(dccProjectCode)
        .matchedSampleId(matchedSampleId)
        .aliquotId(aliquotId)
        .isUsProject(isUsProject)
        .dataType(dataType)
        .workflowType(workflowType)
        .build();
  }

  private Stream<Uuid2BarcodeSheetModel> createSubmitterSampleIdStream(String submitterSampleId){
    return uuid2BarcodeSheetList.stream()
        .filter(s -> s.getUuid().equals(submitterSampleId));
  }

  private Stream<SampleSheetModel> createDonorUniqueIdStream(String donorUniqueId){
    return sampleSheetList.stream()
        .filter(x -> x.getDonorUniqueId().equals(donorUniqueId))
        .filter(y -> y.getLibraryStrategy().equals(WGS))
        .filter(z -> z.getDccSpecimenType().toLowerCase().contains(NORMAL));
  }

  private List<SampleSheetModel> readSampleSheet(final boolean hasHeader){
    return readTsv(sampleSheetFilename, hasHeader, SampleSheetModel::newSampleSheetModel);
  }

  private List<Uuid2BarcodeSheetModel> readUuid2BarcodeSheet( final boolean hasHeader){
    return readTsv(uuid2BarcodeSheetFilename, hasHeader, Uuid2BarcodeSheetModel::newUuid2BarcodeSheetModel);
  }



}
