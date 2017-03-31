package org.icgc.dcc.pcawg.client.download;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.data.FileSampleMetadataDAO_old;
import org.junit.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.pcawg.client.data.FileSampleMetadataDAO_old.newFileSampleMetadataDAO_old;
import static org.icgc.dcc.pcawg.client.model.metadata.project.SampleSheetModel.newSampleSheetModel;
import static org.icgc.dcc.pcawg.client.model.metadata.project.Uuid2BarcodeSheetModel.newUuid2BarcodeSheetModel;

@Slf4j
public class CustomCsvParserTest {

  private static final String TEST_SAMPLE_SHEET_FILENAME = "fixtures/test_sample_sheet.tsv";
  private static final String TEST_UUID2BARCODE_SHEET_FILENAME = "fixtures/test_uuid2barcode_sheet.tsv";

  @Test
  public void testSampleSheetParser(){
    val tsvLine = "  \ta spaceA 1\tb spaceB 2\tc spaceC 3\td spaceD 4\te spaceE 5\tf spaceF 6\tg spaceG 7\th spaceH 8\ti spaceI 9\tj spaceJ 10\tk spaceK 11\tl spaceL 12\t         ";

    val sampleSheet = newSampleSheetModel(tsvLine);
    assertThat(sampleSheet.getAliquot_id()).isEqualTo("f spaceF 6");
    assertThat(sampleSheet.getDccProjectCode()).isEqualTo("e spaceE 5");
    assertThat(sampleSheet.getDccSpecimenType()).isEqualTo("k spaceK 11");
    assertThat(sampleSheet.getDonorUniqueId()).isEqualTo("a spaceA 1");
    assertThat(sampleSheet.getLibraryStrategy()).isEqualTo("l spaceL 12");
    assertThat(sampleSheet.getSubmitterSampleId()).isEqualTo("i spaceI 9");
  }

  @Test
  public void testUuid2BarcodeSheetParser() {
    val tsvLine = "  \ta spaceA 1\tb spaceB 2\tc spaceC 3\td spaceD 4\t   ";
    val sheet = newUuid2BarcodeSheetModel(tsvLine);
    assertThat(sheet.getUuid()).isEqualTo("c spaceC 3");
    assertThat(sheet.getTcgaBarcode()).isEqualTo("d spaceD 4");
  }

  @SneakyThrows
  public File getTestFile(String filename){
    val url = Resources.getResource(filename);
    return new File(url.toURI());
  }

  @Test
  public void testHeaderInclusion(){
    val sampleSheetFile = getTestFile(TEST_SAMPLE_SHEET_FILENAME);
    val uuid2BarcodeSheetFile = getTestFile(TEST_UUID2BARCODE_SHEET_FILENAME);
    assertThat(sampleSheetFile).exists();
    assertThat(uuid2BarcodeSheetFile).exists();
    val absSampleSheetFilename = sampleSheetFile.getAbsolutePath();
    val absUuid2BarcodeSheetFilename = uuid2BarcodeSheetFile.getAbsolutePath();

    val numLinesSampleSheet = 9;
    val numLinesBarcodeSheet = 3;

    FileSampleMetadataDAO_old
        fileSampleMetadataDAOOld = newFileSampleMetadataDAO_old(absSampleSheetFilename,true, absUuid2BarcodeSheetFilename, true );
    assertThat(fileSampleMetadataDAOOld.getSampleSheetSize()).isEqualTo(numLinesSampleSheet-1);
    assertThat(fileSampleMetadataDAOOld.getUUID2BarcodeSheetSize()).isEqualTo(numLinesBarcodeSheet-1);

    fileSampleMetadataDAOOld = newFileSampleMetadataDAO_old(absSampleSheetFilename,true, absUuid2BarcodeSheetFilename, false );
    assertThat(fileSampleMetadataDAOOld.getSampleSheetSize()).isEqualTo(numLinesSampleSheet-1);
    assertThat(fileSampleMetadataDAOOld.getUUID2BarcodeSheetSize()).isEqualTo(numLinesBarcodeSheet);

    fileSampleMetadataDAOOld = newFileSampleMetadataDAO_old(absSampleSheetFilename,false, absUuid2BarcodeSheetFilename, true );
    assertThat(fileSampleMetadataDAOOld.getSampleSheetSize()).isEqualTo(numLinesSampleSheet);
    assertThat(fileSampleMetadataDAOOld.getUUID2BarcodeSheetSize()).isEqualTo(numLinesBarcodeSheet-1);

    fileSampleMetadataDAOOld = newFileSampleMetadataDAO_old(absSampleSheetFilename,false, absUuid2BarcodeSheetFilename, false );
    assertThat(fileSampleMetadataDAOOld.getSampleSheetSize()).isEqualTo(numLinesSampleSheet);
    assertThat(fileSampleMetadataDAOOld.getUUID2BarcodeSheetSize()).isEqualTo(numLinesBarcodeSheet);
  }

}
