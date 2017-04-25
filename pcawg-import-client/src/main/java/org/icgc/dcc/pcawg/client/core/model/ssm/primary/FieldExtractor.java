package org.icgc.dcc.pcawg.client.core.model.ssm.primary;

public interface FieldExtractor<T> {

  String extractStringValue(T data);

}
