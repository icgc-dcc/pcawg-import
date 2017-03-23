package org.icgc.dcc.pcawg.client.download;

import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.icgc.dcc.pcawg.client.config.ClientProperties;
import org.icgc.dcc.pcawg.client.core.writer.HdfsFileWriter;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

@Slf4j
public class HDFSWriterTest {

  @Ignore
  @Test
  public void testWrite() throws IOException{
    val start = "hdfs://"+ClientProperties.HDFS_ADDRESS+":"+ClientProperties.HDFS_PORT;
    val filename = "/hdfs/tmp/rob_test.txt";
    val hostname = "localhost";
    val port = "8020";
    val append = true;
    val outputFilename = start+filename;
    System.out.println("outputFilename: "+outputFilename);
    val writer = HdfsFileWriter.newDefaultHdfsFileWriter(hostname, port, outputFilename, append);
      System.out.println("writing");
      writer.write("hello this is rob");
  }

}
