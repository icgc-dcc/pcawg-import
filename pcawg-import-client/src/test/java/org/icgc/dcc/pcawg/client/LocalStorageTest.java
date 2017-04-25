package org.icgc.dcc.pcawg.client;

import com.google.common.io.Resources;
import lombok.SneakyThrows;
import lombok.val;
import org.assertj.core.util.Maps;
import org.icgc.dcc.pcawg.client.core.model.portal.PortalMetadata;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.icgc.dcc.pcawg.client.core.model.portal.PortalFilename.newPortalFilename;
import static org.icgc.dcc.pcawg.client.filter.variant.VariantFilterFactory.newVariantFilterFactory;
import static org.icgc.dcc.pcawg.client.storage.impl.LocalStorage.newLocalStorage;
import static org.icgc.dcc.pcawg.client.vcf.VCF.newDefaultVCFEncoder;
import static org.icgc.dcc.pcawg.client.vcf.VCF.newDefaultVCFFileReader;

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


  @Test
  @SneakyThrows
  @Ignore
  public void testFilter(){
    val printer = new FileWriter("rob.vcf", false);
    val varFiltFactory = newVariantFilterFactory(false, true);
    val dirpath = Paths.get("/Users/rtisma/Documents/oicr/pcawgConsensusVCFs/pcawg_consensus_vcf").toAbsolutePath();
    val map =  Maps.<String, Boolean>newHashMap();
    map.put("f83fc777-5416-c3e9-e040-11ac0d482c8e.consensus.20160830.somatic.snv_mnv.vcf.gz", false);
    map.put("cf5deb22-f7eb-409d-a0e4-882716199c39.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("0ed63d84-d3fe-4289-9255-35f4a03b703b.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("642e1379-1061-40bc-9a4c-f7c191e84d9d.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("33b7e799-ad10-498f-9948-8ce433311539.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("5ecc88f7-8391-4168-af11-07a6bf9b3652.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("a9dbd55c-5dcc-48db-8785-6baef3fdd7db.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("483bb781-0179-42e1-bf9c-487b240769b8.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("c504d5a9-29b0-4b7e-ac7b-5e543449a0f4.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("42f88b95-fa12-47c7-93f1-cf72f207291c.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("6ba5f81a-b7a0-4c18-a112-2e11094eec85.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("ca2b9fe2-97e0-4d4f-afd7-a5acf638800f.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("f03330dd-c616-4ad5-abc3-6d6b0445e9e9.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("e6801359-d1d7-4871-b2fb-180674a2e469.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("dddea2e4-b8c3-4157-9d92-6de472e8375a.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("63d0c49d-918d-41fb-808a-1f8001981917.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("c14407ad-670e-4d1e-9417-2b76f4810fff.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("7815397b-aa39-4b79-bcaa-6859a3f115f8.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("72293a70-9dc8-4e4a-acdc-c74587a90420.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("19d6cf34-1cd7-4242-a4d1-5d3e11f428aa.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("2425a532-f562-423a-88f5-228642f53875.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("ad9455e9-7147-489e-9b1f-3540c457c260.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("e1217ebe-1826-41a9-b6c4-702100a66f5e.consensus.20160830.somatic.snv_mnv.vcf.gz",true);
    map.put("8fb7fcac-6c1d-40c2-9309-b53821cbef30.consensus.20160830.somatic.snv_mnv.vcf.gz",true);

    map.put("f83fc777-5416-c3e9-e040-11ac0d482c8e.consensus.20160830.somatic.indel.vcf.gz", false);
    map.put("cf5deb22-f7eb-409d-a0e4-882716199c39.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("0ed63d84-d3fe-4289-9255-35f4a03b703b.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("642e1379-1061-40bc-9a4c-f7c191e84d9d.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("33b7e799-ad10-498f-9948-8ce433311539.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("5ecc88f7-8391-4168-af11-07a6bf9b3652.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("a9dbd55c-5dcc-48db-8785-6baef3fdd7db.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("483bb781-0179-42e1-bf9c-487b240769b8.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("c504d5a9-29b0-4b7e-ac7b-5e543449a0f4.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("42f88b95-fa12-47c7-93f1-cf72f207291c.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("6ba5f81a-b7a0-4c18-a112-2e11094eec85.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("ca2b9fe2-97e0-4d4f-afd7-a5acf638800f.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("f03330dd-c616-4ad5-abc3-6d6b0445e9e9.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("e6801359-d1d7-4871-b2fb-180674a2e469.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("dddea2e4-b8c3-4157-9d92-6de472e8375a.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("63d0c49d-918d-41fb-808a-1f8001981917.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("c14407ad-670e-4d1e-9417-2b76f4810fff.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("7815397b-aa39-4b79-bcaa-6859a3f115f8.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("72293a70-9dc8-4e4a-acdc-c74587a90420.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("19d6cf34-1cd7-4242-a4d1-5d3e11f428aa.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("2425a532-f562-423a-88f5-228642f53875.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("ad9455e9-7147-489e-9b1f-3540c457c260.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("e1217ebe-1826-41a9-b6c4-702100a66f5e.consensus.20160830.somatic.indel.vcf.gz",true);
    map.put("8fb7fcac-6c1d-40c2-9309-b53821cbef30.consensus.20160830.somatic.indel.vcf.gz",true);

    int foundNum = 0;
    int foundMax = 3;
    int notFoundNum = 0;
    int notFoundMax = 3;
    for (val entry : map.entrySet()){
      val filename = entry.getKey();
      val isUsProject = entry.getValue();

      val path = dirpath.resolve(Paths.get(filename));
      val portalMetadata = createPortalMetadata(path,"2234234incorrectChecksum22838" );
      val file = path.toFile();

      val vcfFileReader  = newDefaultVCFFileReader(file);
      val vcfEncoder = newDefaultVCFEncoder(vcfFileReader);
      val varFilter  = varFiltFactory.createVariantFilter(vcfEncoder, isUsProject);


      for (val variant : vcfFileReader){
        val variantString = vcfEncoder.encode(variant);
        val isFiltered = varFilter.passedAllFilters(variant);
        if (isFiltered){
          if(foundNum < foundMax){
            printer.write("FILTERED\t"+variantString+"\n");
            foundNum++;
          }
        } else {
          if(notFoundNum < notFoundMax){
            printer.write("NOT_FILTERED\t"+variantString+"\n");
            notFoundNum++;
          }
        }
        if ((foundNum == foundMax) && (notFoundNum == notFoundMax)){
          break;
        }
      }
    }
    printer.close();
    varFiltFactory.close();





  }

}
