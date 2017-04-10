package org.icgc.dcc.pcawg.client.model.ssm.primary;

public interface FieldExtractor<T> {

  String extractStringValue(T data);

}
