package org.icgc.dcc.pcawg.client.data;

import java.util.List;

public interface BasicDao<B, R> {

  List<B> find(R request);

  List<B> findAll();

}
