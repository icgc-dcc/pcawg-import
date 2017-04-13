package org.icgc.dcc.pcawg.client.download;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.pcawg.client.core.PersistedFactory.newPersistedFactory;
import static org.icgc.dcc.pcawg.client.data.portal.PortalFilename.newPortalFilename;

@Slf4j
public class SampleMetadataDAOTest {

  @SneakyThrows
  private void runQuery(boolean useFast){
    val sampleMetadataDAO = newPersistedFactory(useFast).newSampleMetadataDAO();
    val nonUsFilename = "10cb8ac6-c622-11e3-bf01-24c6515278c0.dkfz-copyNumberEstimation_1-0-189-hpc-fix.1508271624.somatic.cnv.vcf.gz";
    val usFilename = "9c70688d-6e43-4520-9262-eaae4e4d597d.broad-snowman.20150827.somatic.sv.vcf.gz";
    val nonUsId = newPortalFilename(nonUsFilename);
    val usId = newPortalFilename(usFilename);

    //Non-US
    val nonUsProjectData = sampleMetadataDAO.fetchSampleMetadata(nonUsId);
    assertThat(nonUsProjectData.getDccProjectCode()).isEqualTo("LIRI-JP");
    assertThat(nonUsProjectData.getAnalyzedSampleId()).isEqualTo("RK001_C01");
    assertThat(nonUsProjectData.getMatchedSampleId()).isEqualTo("RK001_B01");

    /**
     * TODO: [DCC-5532] Currently failing becuase ICGC does not contain US projects that have VCFs and BAMs mapped to the same AliquotId
     */
    //US
    val usProjectData = sampleMetadataDAO.fetchSampleMetadata(usId);
    assertThat(usProjectData.getDccProjectCode()).isEqualTo("BRCA-US");
    assertThat(usProjectData.getAnalyzedSampleId()).isEqualTo("TCGA-BH-A18R-01A-11D-A19H-09");
    assertThat(usProjectData.getMatchedSampleId()).isEqualTo("TCGA-BH-A18R-11A-42D-A19H-09");
  }

  @Test
  @SneakyThrows
  public void testFastFetchSampleMetadata(){
    val useFast = true;
    runQuery(useFast);
  }

  @Test
  @SneakyThrows
  public void testBeanFetchSampleMetadata() {
    val useFast = false;
    runQuery(useFast);
  }

  public void runFileIdDaoTest(String input1, String expectedOutput1, String input2, String expectedOutput2){
    val icgcDao = newPersistedFactory(true).buildFileId();

        val result1 = icgcDao.find(input1);
    assertThat(result1.isPresent()).isTrue();
    assertThat(result1.get()).isEqualTo(expectedOutput1);

    val result2 = icgcDao.find(input2);
    assertThat(result2.isPresent()).isTrue();
    assertThat(result2.get()).isEqualTo(expectedOutput2);
  }

  @Test
  @SneakyThrows
  public void testUsIcgcFileIdDao(){
    val input1 = "TCGA-FF-8046-01A-11D-2210-10";
    val expectedOutput1 = "FI9956";
    val input2 = "TCGA-FF-8046-10A-01D-2210-10";
    val expectedOutput2 = "FI9955";
    runFileIdDaoTest(input1, expectedOutput1, input2, expectedOutput2);
  }

  @Test
  @SneakyThrows
  public void testNonUsIcgcFileIdDao(){
    val input1 = "PD4982a";
    val expectedOutput1= "FI9995";
    val input2= "PD4982b";
    val expectedOutput2= "FI9994";
    runFileIdDaoTest(input1, expectedOutput1, input2, expectedOutput2);
  }


}
