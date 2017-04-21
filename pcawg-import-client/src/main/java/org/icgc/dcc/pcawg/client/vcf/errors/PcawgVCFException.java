package org.icgc.dcc.pcawg.client.vcf.errors;

public class PcawgVCFException extends PcawgErrorException {

  public PcawgVCFException(String filename, String message) {
    super(message);
    this.filename = filename;
  }

  public PcawgVCFException(String filename, String message, Throwable cause) {
    super(message, cause);
    this.filename = filename;
  }

  public PcawgVCFException(String filename, Throwable cause) {
    super(cause);
    this.filename = filename;
  }

  private final String filename;

}
