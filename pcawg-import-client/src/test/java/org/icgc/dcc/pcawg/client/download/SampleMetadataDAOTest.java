package org.icgc.dcc.pcawg.client.download;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBean;
import org.icgc.dcc.pcawg.client.data.barcode.BarcodeBeanDao;
import org.icgc.dcc.pcawg.client.data.sample.SampleBean;
import org.icgc.dcc.pcawg.client.data.sample.SampleBeanDao;
import org.junit.Test;

import java.io.File;
import java.io.FileReader;

import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.pcawg.client.core.Factory.newSampleMetadataDAO;
import static org.icgc.dcc.pcawg.client.model.metadata.file.PortalFilename.newPortalFilename;

@Slf4j
public class SampleMetadataDAOTest {

  private static final String TEST_SAMPLE_SHEET_FILENAME = "fixtures/test_sample_sheet.tsv";
  private static final String TEST_UUID2BARCODE_SHEET_FILENAME = "fixtures/test_uuid2barcode_sheet.tsv";

  @Test
  @SneakyThrows
  public void testSampleDao(){
    val reader = new FileReader(getTestFile(TEST_SAMPLE_SHEET_FILENAME));
    val dao = SampleBeanDao.newSampleBeanDao(reader);
    val aliquotId = "e0fccaf5-925a-41f9-b87c-cd5ee4aecb59";
    log.info("AliquotId: {}", aliquotId);
    log.info("fff: {}",
        dao.findAliquotId(aliquotId)
        .stream()
        .map(SampleBean::toString)
        .collect(joining("\n")) );
  }

  @Test
  @SneakyThrows
  public void testBarcodeDao(){
    val reader = new FileReader(getTestFile(TEST_UUID2BARCODE_SHEET_FILENAME));
    val dao = BarcodeBeanDao.newBarcodeBeanDao(reader);
    val uuid = "0304b12d-7640-4150-a581-2eea2b1f2ad5";
    log.info("UUID: {}", uuid);
    log.info(dao.find(uuid)
        .stream()
        .map(BarcodeBean::toString)
        .collect(joining("\n")));

  }


  @SneakyThrows
  public File getTestFile(String filename){
    val url = Resources.getResource(filename);
    return new File(url.toURI());
  }

  @Test
  @SneakyThrows
  public void testFetchSampleMetadata(){
    val sampleMetadataDAO = newSampleMetadataDAO();
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


}
