package org.icgc.dcc.pcawg.client.vcf.errors;

import htsjdk.variant.variantcontext.VariantContext;
import lombok.Getter;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

public class PcawgVariantException extends PcawgException {

  @Getter
  private final List<PcawgVariantErrors> errors;

  @Getter
  private final VariantContext variant;

  public PcawgVariantException(String message, VariantContext variant, PcawgVariantErrors error) {
    this(message, variant, newArrayList(error));
  }
  public PcawgVariantException(String message, VariantContext variant, List<PcawgVariantErrors> errors) {
    super(message);
    this.variant = variant;
    this.errors = errors;
  }

  public PcawgVariantException(String message, Throwable cause, VariantContext variant, PcawgVariantErrors error) {
    this(message, cause, variant, newArrayList(error));
  }
  public PcawgVariantException(String message, Throwable cause, VariantContext variant, List<PcawgVariantErrors> errors) {
    super(message, cause);
    this.variant = variant;
    this.errors = errors;
  }

  public PcawgVariantException(Throwable cause, VariantContext variant, PcawgVariantErrors error) {
    this(cause, variant, newArrayList(error));
  }
  public PcawgVariantException(Throwable cause, VariantContext variant, List<PcawgVariantErrors> errors) {
    super(cause);
    this.variant = variant;
    this.errors = errors;
  }

}
