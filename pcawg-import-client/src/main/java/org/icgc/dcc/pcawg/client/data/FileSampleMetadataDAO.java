package org.icgc.dcc.pcawg.client.data;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.model.metadata.file.FilenameParser;
import org.icgc.dcc.pcawg.client.model.metadata.project.SampleMetadata;
import org.icgc.dcc.pcawg.client.model.metadata.project.SampleSheetModel;
import org.icgc.dcc.pcawg.client.model.metadata.project.Uuid2BarcodeSheetModel;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.pcawg.client.data.SampleMetadataDAO.isUSProject;

@Slf4j
public class FileSampleMetadataDAO implements SampleMetadataDAO {

  private static final String WGS = "WGS";
  private static final String NORMAL = "normal";

  @NonNull
  private final String sampleSheetFilename;

  @NonNull
  private final String uuid2BarcodeSheetFilename;

  private final List<SampleSheetModel> sampleSheetList;
  private final List<Uuid2BarcodeSheetModel> uuid2BarcodeSheetList;

  public FileSampleMetadataDAO(String sampleSheetFilename, String uuid2BarcodeSheetFilename) {
    this.sampleSheetFilename = sampleSheetFilename;
    this.uuid2BarcodeSheetFilename = uuid2BarcodeSheetFilename;
    this.sampleSheetList = readSampleSheet();
    this.uuid2BarcodeSheetList = readUuid2BarcodeSheet();
  }

  private SampleSheetModel getFirstSampleSheetByAliquotId(String aliquotId){
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
  public SampleMetadata getSampleMetadataByFilenameParser(FilenameParser filenameParser){
    val aliquotId = filenameParser.getAliquotId();
    val workflow =  filenameParser.getWorkflow();
    val dataType =  filenameParser.getDataType();
    val sampleSheetByAliquotId = getFirstSampleSheetByAliquotId(aliquotId);
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
        .workflow(workflow)
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

  private List<SampleSheetModel> readSampleSheet(){
    return readTsv(sampleSheetFilename, true, SampleSheetModel::newSampleSheetModelFromTSVLine);
  }

  private List<Uuid2BarcodeSheetModel> readUuid2BarcodeSheet(){
    return readTsv(uuid2BarcodeSheetFilename, true, Uuid2BarcodeSheetModel::newUuid2BarcodeSheetModelFromTSVLine);
  }

  @SneakyThrows
  private static <T> List<T> readTsv(@NonNull  String filename,
      final boolean hasHeader,
      @NonNull Function<String, T > lineConvertionFunctor){
    val file = new File(filename);
    checkState(file.exists(), "File %s DNE", filename);
    val br = new BufferedReader(new FileReader(file));
    String line;
    val builder = ImmutableList.<T>builder();
    boolean skipLine = hasHeader;
    while((line = br.readLine()) != null){
      if (!skipLine) {
        builder.add(lineConvertionFunctor.apply(line));
      }
      skipLine = false;
    }
    br.close();
    return builder.build();
  }

  public static FileSampleMetadataDAO newFileSampleMetadataDAO(String sampleSheetFilename, String uuid2BarcodeSheetFilename){
    return new FileSampleMetadataDAO(sampleSheetFilename, uuid2BarcodeSheetFilename);
  }

}