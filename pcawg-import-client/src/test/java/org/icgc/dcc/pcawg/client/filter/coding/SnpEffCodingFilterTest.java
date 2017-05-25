package org.icgc.dcc.pcawg.client.filter.coding;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class SnpEffCodingFilterTest {

  @Test
  public void isCodingTest() throws Exception {
    val filter = new SnpEffCodingFilter(); // This takes a minute or so to bootstrap

    // This vcf line is just a bunch of intron variants (non-coding variant)
    val coding = filter.isCoding("1\t15292412\t.\tAATC\tA\t.\t.\tCallers=foo,bar;NumCallers=2;repeat_masker=Charlie1;VAF=0.1923;t_alt_count=15;t_ref_count=63;model_score=0.98;Variant_Classification=Intron");

    assertThat(coding).isEqualTo(false);

    //cleanup snpeff process.
    filter.destroy();
  }

}