package org.icgc.dcc.pcawg.client;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.util.Maps;
import org.icgc.dcc.pcawg.client.data.portal.PortalMetadata;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.icgc.dcc.pcawg.client.data.portal.PortalFilename.newPortalFilename;
import static org.icgc.dcc.pcawg.client.download.LocalStorage.newLocalStorage;

public class LocalStorageTest {

  private static final String LOCAL_STORAGE_DIR_PATH = "fixtures/localStorage";

  @SneakyThrows
  private static Path getPath(String filename){
    val url = Resources.getResource(filename);
    return new File(url.toURI()).toPath();
  }

  private PortalMetadata createPortalMetadata(Path vcfPathname, String md5Checksum){
    val filename = vcfPathname.getFileName().toString();
    val portalFilename = newPortalFilename(filename);
    return PortalMetadata.builder()
        .portalFilename(portalFilename)
        .dataType("myDataType")
        .donorId("myDonorId")
        .fileId("myFileId")
        .fileMd5sum(md5Checksum)
        .fileSize(0)
        .genomeBuild("myGenomeBuild")
        .objectId("myObjectId")
        .referenceName("myReferenceName")
        .sampleId("mySampleId")
        .build();
  }

  private Map<Path, PortalMetadata> buildTestMap(){
    val map = Maps.<Path, PortalMetadata>newHashMap();
    val path1 = Paths.get("f221c897-6ad0-0df9-e040-11ac0c4813ef.consensus.20160830.somatic.indel.vcf.gz");
    val pm1 = createPortalMetadata(path1,"5149d403009a139c7e085405ef762e1a" );
    map.put(path1, pm1);

    val path2 = Paths.get("f82d213f-9843-28eb-e040-11ac0d483e48.consensus.20160830.somatic.snv_mnv.vcf.gz");
    val pm2 = createPortalMetadata(path2, "3d709e89c8ce201e3c928eb917989aef");
    map.put(path2, pm2);

    val path3 = Paths.get("f82d213f-bc06-5b51-e040-11ac0c48687e.consensus.20160830.somatic.indel.vcf.gz");
    val pm3 = createPortalMetadata(path3,"60b91f1875424d3b4322b0fdd0529d5d");
    map.put(path3, pm3);
    return map;
  }

  @Test
  public void testSanityMd5Check(){
    val dirpath = getPath(LOCAL_STORAGE_DIR_PATH);
    val testMap = buildTestMap();
    val bypassMd5Check = false;
    val storage = newLocalStorage(dirpath,bypassMd5Check);
    for (val path : testMap.keySet()){
      val portalMetadata = testMap.get(path);
      val file = storage.getFile(portalMetadata);
    }

  }

  private void runMismatchChecksumTest(boolean bypassMd5Check){
    val dirpath = getPath(LOCAL_STORAGE_DIR_PATH);
    val path = Paths.get("f221c897-6ad0-0df9-e040-11ac0c4813ef.consensus.20160830.somatic.indel.vcf.gz");
    val portalMetadata = createPortalMetadata(path,"2234234incorrectChecksum22838" );
    val storage = newLocalStorage(dirpath,bypassMd5Check);
    val file = storage.getFile(portalMetadata);
  }

  @Test(expected = IllegalStateException.class)
  public void testMismatchMd5Check(){
    val bypassMd5Check = false;
    runMismatchChecksumTest(bypassMd5Check);
  }

  @Test
  public void testMismatchAndBypassMd5Check(){
    val bypassMd5Check = true;
    runMismatchChecksumTest(bypassMd5Check);
  }

}
