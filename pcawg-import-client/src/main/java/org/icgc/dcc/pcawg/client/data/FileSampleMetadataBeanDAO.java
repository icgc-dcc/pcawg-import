package org.icgc.dcc.pcawg.client.data;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBean;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest;
import org.icgc.dcc.pcawg.client.model.metadata.file.PortalFilename;
import org.icgc.dcc.pcawg.client.model.metadata.project.SampleMetadata;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.pcawg.client.data.SampleMetadataDAO.isUSProject;
import static org.icgc.dcc.pcawg.client.data.sample.SampleBeanDaoOld.createWildcardRequestBuilder;

@Slf4j
@RequiredArgsConstructor
public class FileSampleMetadataBeanDAO implements SampleMetadataDAO {

  private static final String WGS = "WGS";
  private static final String NORMAL = "normal";
  private static final boolean F_CHECK_CORRECT_WORKTYPE = true;
  private static final boolean F_CHECK_CORRECT_DATATYPE = true;


  @NonNull
  private final SampleDao<SampleBean, SampleSearchRequest> sampleDao;

  @NonNull
  private final BarcodeDao<BarcodeBean, String> barcodeDao;

  private SampleBean getFirstSampleBean(String aliquotId) throws SampleMetadataNotFoundException{
    val aliquotIdResult = sampleDao.findFirstAliquotId(aliquotId);
    if (! aliquotIdResult.isPresent()){
      throw new SampleMetadataNotFoundException(
      String.format("Could not find first SampleBean for aliquot_id [%s]", aliquotId));

    }
    return aliquotIdResult.get();
  }

  private SampleBean getFirstSampleBean(SampleSearchRequest request) throws SampleMetadataNotFoundException{
    val beans = sampleDao.find(request);
    if (beans.isEmpty()){
      throw new SampleMetadataNotFoundException(
          String.format("Could not find first SampleBean for SampleSearchRequest: %s", request ));
    }
    return getFirst(beans);
  }

  private BarcodeBean getFirstBarcodeBean(String uuid) throws  SampleMetadataNotFoundException {
    val beans = barcodeDao.find(uuid);
    if(beans.isEmpty()){
      throw new SampleMetadataNotFoundException(
          String.format("Could not find first BarcodeBean for uuid [%s]", uuid));
    }
    return getFirst(beans);
  }

  private static <T> T getFirst(List<T> list){
    checkArgument(list.size()>0, "The list is empty");
    return list.get(0);
  }


  @SneakyThrows
  private String getAnalyzedSampleId(boolean isUsProject, String submitterSampleId ) {
    if (isUsProject){
      return getFirstBarcodeBean(submitterSampleId).getBarcode();
    } else {
      return submitterSampleId;
    }
  }

  @SneakyThrows
  private String getMatchedSampleId(boolean isUsProject, String donorUniqueId ){
    val request = createWildcardRequestBuilder()
        .donor_unique_id(donorUniqueId)
        .dcc_specimen_type(NORMAL)
        .library_strategy(WGS)
        .build();
    val result = getFirstSampleBean(request);
    val submitterSampleId = result.getSubmitter_sample_id();
    return getAnalyzedSampleId(isUsProject, submitterSampleId);
  }

  @Override
  public SampleMetadata fetchSampleMetadata(PortalFilename portalFilename) throws SampleMetadataNotFoundException{
    val aliquotId = portalFilename.getAliquotId();
    val workflowType = WorkflowTypes.parseStartsWith(portalFilename.getWorkflow(), F_CHECK_CORRECT_WORKTYPE);
    val dataType = DataTypes.parseMatch(portalFilename.getDataType(), F_CHECK_CORRECT_DATATYPE);
    val sampleSheetByAliquotId = getFirstSampleBean(aliquotId);
    val dccProjectCode = sampleSheetByAliquotId.getDcc_project_code();
    val submitterSampleId = sampleSheetByAliquotId.getSubmitter_sample_id();
    val donorUniqueId = sampleSheetByAliquotId.getDonor_unique_id();
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




}
