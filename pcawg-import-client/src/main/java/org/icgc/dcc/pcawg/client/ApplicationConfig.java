package org.icgc.dcc.pcawg.client;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;

import static org.icgc.dcc.pcawg.client.config.ClientProperties.OUTPUT_TSV_DIRECTORY;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.STORAGE_BYPASS_MD5_CHECK;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.STORAGE_OUTPUT_VCF_STORAGE_DIR;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.STORAGE_PERSIST_MODE;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.TOKEN;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.USE_HDFS;

@ConfigurationProperties
@Setter
@Getter
@ToString
public class ApplicationConfig {

      @NonNull
      private String token = TOKEN;

      private boolean hdfs = USE_HDFS;

      @NonNull
      private String vcf_dir = STORAGE_OUTPUT_VCF_STORAGE_DIR;

      private boolean persist = STORAGE_PERSIST_MODE;

      private boolean bypass_md5 = STORAGE_BYPASS_MD5_CHECK;

      @NonNull
      private String tsv_dir = OUTPUT_TSV_DIRECTORY;

      @NonNull
      private String hdfs_hostname = "localhost";

      @NonNull
      private String hdfs_port = "8020";

}
