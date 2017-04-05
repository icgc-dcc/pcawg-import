package org.icgc.dcc.pcawg.client.data.sample;

import java.util.List;
import java.util.Optional;

public interface SampleSheetDao<B, R> {

  List<B> find(R request);

  Optional<B> findFirstAliquotId(String aliquotId);

  List<B> findAliquotId(String aliquotId);

  Optional<B> findFirstDonorUniqueId(String donorUniqueId);

  List<B> findDonorUniqueId(String donorUniqueId);

  List<B> findAll();

}
