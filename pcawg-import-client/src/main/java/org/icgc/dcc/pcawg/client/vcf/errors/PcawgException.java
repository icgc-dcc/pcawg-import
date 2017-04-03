package org.icgc.dcc.pcawg.client.vcf.errors;

public class PcawgException extends RuntimeException {

  public PcawgException(String message) {
    super(message);
  }

  public PcawgException(String message, Throwable cause) {
    super(message, cause);
  }

  public PcawgException(Throwable cause) {
    super(cause);
  }

}
