package org.icgc.dcc.pcawg.client.tsv.converter;

import java.util.stream.Collector;
import java.util.stream.Collectors;

public interface TSVConverter<T> {

  static final Collector<CharSequence, ?, String> TSV_STRING_COLLECTOR = Collectors.joining("\t");

  default Collector<CharSequence,?, String> getTsvCollectorInstance(){
    return TSV_STRING_COLLECTOR;
  }

  String toTSVData(T t);

  String toTSVHeader();

}
