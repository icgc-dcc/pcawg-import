package org.icgc.dcc.pcawg.client.data.sample;

import org.icgc.dcc.pcawg.client.data.BasicDao;

import java.util.List;
import java.util.Optional;

public interface SampleSheetDao<B, R> extends BasicDao<B, R>{

  Optional<B> findFirstAliquotId(String aliquotId);

  List<B> findAliquotId(String aliquotId);

  Optional<B> findFirstDonorUniqueId(String donorUniqueId);

  List<B> findDonorUniqueId(String donorUniqueId);

}
