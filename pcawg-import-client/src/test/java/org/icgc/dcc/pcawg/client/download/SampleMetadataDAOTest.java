package org.icgc.dcc.pcawg.client.download;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.SampleMetadataDAO;
import org.junit.Ignore;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.pcawg.client.core.Factory.newFileSampleMetadataBeanDAOAndDownload;
import static org.icgc.dcc.pcawg.client.core.Factory.newFileSampleMetadataFastDAOAndDownload;
import static org.icgc.dcc.pcawg.client.model.metadata.file.PortalFilename.newPortalFilename;

@Slf4j
public class SampleMetadataDAOTest {


  @SneakyThrows
  private void runQuery(SampleMetadataDAO sampleMetadataDAO){
    val nonUsFilename = "10cb8ac6-c622-11e3-bf01-24c6515278c0.dkfz-copyNumberEstimation_1-0-189-hpc-fix.1508271624.somatic.cnv.vcf.gz";
    val usFilename = "9c70688d-6e43-4520-9262-eaae4e4d597d.broad-snowman.20150827.somatic.sv.vcf.gz";
    val nonUsId = newPortalFilename(nonUsFilename);
    val usId = newPortalFilename(usFilename);

    //Non-US
    val nonUsProjectData = sampleMetadataDAO.fetchSampleMetadata(nonUsId);
    assertThat(nonUsProjectData.getDccProjectCode()).isEqualTo("LIRI-JP");
    assertThat(nonUsProjectData.getAnalyzedSampleId()).isEqualTo("RK001_C01");
    assertThat(nonUsProjectData.getMatchedSampleId()).isEqualTo("RK001_B01");

    //US
    val usProjectData = sampleMetadataDAO.fetchSampleMetadata(usId);
    assertThat(usProjectData.getDccProjectCode()).isEqualTo("BRCA-US");
    assertThat(usProjectData.getAnalyzedSampleId()).isEqualTo("TCGA-BH-A18R-01A-11D-A19H-09");
    assertThat(usProjectData.getMatchedSampleId()).isEqualTo("TCGA-BH-A18R-11A-42D-A19H-09");
  }

  @Test
  @SneakyThrows
  public void testFastFetchSampleMetadata(){
    runQuery(newFileSampleMetadataFastDAOAndDownload());
  }

  @Test
  @SneakyThrows
  @Ignore("Very slow")
  public void testBeanFetchSampleMetadata(){
    runQuery(newFileSampleMetadataBeanDAOAndDownload());
  }


}
