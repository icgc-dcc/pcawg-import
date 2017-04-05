package org.icgc.dcc.pcawg.client.data.metadata.impl;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.icgc.FileIdDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeDao;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeSearchRequest;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadataDAO;
import org.icgc.dcc.pcawg.client.data.metadata.SampleMetadataNotFoundException;
import org.icgc.dcc.pcawg.client.data.sample.SampleDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleSearchRequest;
import org.icgc.dcc.pcawg.client.model.beans.BarcodeBean;
import org.icgc.dcc.pcawg.client.model.beans.SampleBean;
import org.icgc.dcc.pcawg.client.model.metadata.file.PortalFilename;
import org.icgc.dcc.pcawg.client.model.metadata.project.SampleMetadata;
import org.icgc.dcc.pcawg.client.vcf.DataTypes;
import org.icgc.dcc.pcawg.client.vcf.WorkflowTypes;

import static com.google.common.base.Preconditions.checkState;
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

  @NonNull
  private final FileIdDao fileIdDao;

  private SampleBean getFirstSampleBean(String aliquotId) throws SampleMetadataNotFoundException {
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

    return SampleMetadata.builder()
        .analyzedSampleId(analyzedSampleId)
        .dccProjectCode(dccProjectCode)
        .matchedSampleId(matchedSampleId)
        .aliquotId(aliquotId)
        .isUsProject(isUsProject)
        .dataType(dataType)
        .workflowType(workflowType)
        .analyzedFileId(analyzedFileId)
        .matchedFileId(matchedFileId)
        .build();
  }

}
