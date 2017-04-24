package org.icgc.dcc.pcawg.client.data.metadata.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSheetBean;
import org.icgc.dcc.pcawg.client.data.BasicDao;
import org.icgc.dcc.pcawg.client.data.icgc.FileIdDao;
import org.icgc.dcc.pcawg.client.data.metadata.BasicSampleMetadata;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadata;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadataDAO;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadataNotFoundException;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSheetSearchRequest;
import org.icgc.dcc.pcawg.client.data.portal.PortalFilename;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest.newBarcodeRequest;
import static org.icgc.dcc.pcawg.client.data.icgc.IdResolver.getAnalyzedSampleId;
import static org.icgc.dcc.pcawg.client.data.icgc.IdResolver.getMatchedSampleId;

@Slf4j
@RequiredArgsConstructor
@Value
public class FileSampleMetadataBeanDAO implements SampleMetadataDAO {

  private static final boolean F_CHECK_CORRECT_WORKTYPE = true;
  private static final boolean F_CHECK_CORRECT_DATATYPE = true;


  @NonNull
  private final SampleSheetDao<SampleSheetBean, SampleSheetSearchRequest> sampleSheetDao;

  @NonNull
  private final BasicDao<BarcodeSheetBean, BarcodeSearchRequest> barcodeSheetDao;

  @NonNull
  private final FileIdDao fileIdDao;

  private SampleSheetBean getFirstSampleBean(String aliquotId) throws SampleMetadataNotFoundException {
    val aliquotIdResult = sampleSheetDao.findFirstAliquotId(aliquotId);
    if (! aliquotIdResult.isPresent()){
      throw new SampleMetadataNotFoundException(
      String.format("Could not find first SampleSheetBean for aliquot_id [%s]", aliquotId));

    }
    return aliquotIdResult.get();
  }

  @Override
  public SampleMetadata fetchSampleMetadata(PortalFilename portalFilename) throws SampleMetadataNotFoundException{
    val aliquotId = portalFilename.getAliquotId();
    val workflowType = WorkflowTypes.parseStartsWith(portalFilename.getWorkflow(), F_CHECK_CORRECT_WORKTYPE);
    val sampleBean = getFirstSampleBean(aliquotId);
    val dccProjectCode = sampleBean.getDcc_project_code();
    val submitterSampleId = sampleBean.getSubmitter_sample_id();
    val barcodeSearchRequest = newBarcodeRequest(submitterSampleId);
    val donorUniqueId = sampleBean.getDonor_unique_id();
    val isUsProject =  sampleBean.isUsProject();


    val analyzedSampleId = getAnalyzedSampleId(barcodeSheetDao,isUsProject, barcodeSearchRequest);
    val matchedSampleId = getMatchedSampleId(sampleSheetDao, barcodeSheetDao, donorUniqueId);
    val analyzedFileIdResult = fileIdDao.find(analyzedSampleId);
    val matchedFileIdResult = fileIdDao.find(matchedSampleId);
    checkState(analyzedFileIdResult.isPresent(),
        "The IcgcFileIdDao does not contain the analyzedSampleId (%s) for the vcf file %s ",
        analyzedSampleId,portalFilename.getFilename());
    checkState(matchedFileIdResult.isPresent(),
        "The IcgcFileIdDao does not contain the matchedSampleId (%s) for the vcf file %s ",
        matchedSampleId,portalFilename.getFilename());
    val analyzedFileId = analyzedFileIdResult.get();
    val matchedFileId = matchedFileIdResult.get();

    return BasicSampleMetadata.builder()
        .analyzedSampleId(analyzedSampleId)
        .dccProjectCode(dccProjectCode)
        .matchedSampleId(matchedSampleId)
        .aliquotId(aliquotId)
        .isUsProject(isUsProject)
        .workflowType(workflowType)
        .analyzedFileId(analyzedFileId)
        .matchedFileId(matchedFileId)
        .build();
  }

}
