package org.icgc.dcc.pcawg.client.core;

import com.fasterxml.jackson.databind.node.ObjectNode;

import static org.icgc.dcc.common.core.json.JsonNodeBuilders.array;
import static org.icgc.dcc.common.core.json.JsonNodeBuilders.object;

public interface ObjectNodeConverter {

  ObjectNode toObjectNode();

  default ObjectNode createIs(String ... values){
    return object()
        .with("is",
            array()
                .with(values)
                .end())
        .end();

  }

}
