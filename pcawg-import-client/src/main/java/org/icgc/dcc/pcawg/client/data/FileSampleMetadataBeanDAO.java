package org.icgc.dcc.pcawg.client.data;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBean;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.sample.SampleBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest;
import org.icgc.dcc.pcawg.client.model.metadata.file.PortalFilename;
import org.icgc.dcc.pcawg.client.model.metadata.project.SampleMetadata;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static org.icgc.dcc.pcawg.client.data.IdResolver.getAnalyzedSampleId;
import static org.icgc.dcc.pcawg.client.data.IdResolver.getMatchedSampleId;
import static org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest.newBarcodeRequest;

@Slf4j
@RequiredArgsConstructor
@Value
public class FileSampleMetadataBeanDAO implements SampleMetadataDAO {

  private static final boolean F_CHECK_CORRECT_WORKTYPE = true;
  private static final boolean F_CHECK_CORRECT_DATATYPE = true;


  @NonNull
  private final SampleDao<SampleBean, SampleSearchRequest> sampleDao;

  @NonNull
  private final BarcodeDao<BarcodeBean, BarcodeSearchRequest> barcodeDao;

  private SampleBean getFirstSampleBean(String aliquotId) throws SampleMetadataNotFoundException{
    val aliquotIdResult = sampleDao.findFirstAliquotId(aliquotId);
    if (! aliquotIdResult.isPresent()){
      throw new SampleMetadataNotFoundException(
      String.format("Could not find first SampleBean for aliquot_id [%s]", aliquotId));

    }
    return aliquotIdResult.get();
  }

  @Override
  public SampleMetadata fetchSampleMetadata(PortalFilename portalFilename) throws SampleMetadataNotFoundException{
    val aliquotId = portalFilename.getAliquotId();
    val workflowType = WorkflowTypes.parseStartsWith(portalFilename.getWorkflow(), F_CHECK_CORRECT_WORKTYPE);
    val dataType = DataTypes.parseMatch(portalFilename.getDataType(), F_CHECK_CORRECT_DATATYPE);
    val sampleBean = getFirstSampleBean(aliquotId);
    val dccProjectCode = sampleBean.getDcc_project_code();
    val submitterSampleId = sampleBean.getSubmitter_sample_id();
    val barcodeSearchRequest = newBarcodeRequest(submitterSampleId);
    val donorUniqueId = sampleBean.getDonor_unique_id();
    val isUsProject =  sampleBean.isUsProject();


    val analyzedSampleId = getAnalyzedSampleId(barcodeDao,isUsProject, barcodeSearchRequest);
    val matchedSampleId = getMatchedSampleId(sampleDao, barcodeDao, donorUniqueId);
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
