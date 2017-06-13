package org.icgc.dcc.pcawg.client.download;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import org.icgc.dcc.pcawg.client.core.ObjectNodeConverter;

import java.net.URL;
import java.net.URLEncoder;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.icgc.dcc.pcawg.client.config.ClientProperties.PORTAL_API;

@Builder
public class Portal {

  private static final String REPOSITORY_FILES_ENDPOINT = "/api/v1/repository/files";
  private static final Joiner AMPERSAND_JOINER = Joiner.on("&");
  private static final int PORTAL_FETCH_SIZE = 100;
  private static final String HITS = "hits";
  private static final ObjectMapper DEFAULT_MAPPER = new ObjectMapper();

  @NonNull
  private final ObjectNodeConverter jsonQueryGenerator;

  @SneakyThrows
  public URL getUrl(int size, int from) {
    val endpoint = PORTAL_API + REPOSITORY_FILES_ENDPOINT;
    val include = "facets";
    val filters = URLEncoder.encode(jsonQueryGenerator.toObjectNode().toString(), UTF_8.name());
    val urlEnding = AMPERSAND_JOINER.join(
        "include="+include,
        "from="+from,
        "size="+size,
        "filters="+filters
    );
    return new URL(endpoint + "?" + urlEnding);
  }

  private static JsonNode getHits(JsonNode result) {
    return result.get(HITS);
  }

  public List<ObjectNode> getFileMetas() {
    val fileMetas = ImmutableList.<ObjectNode> builder();
    val size = PORTAL_FETCH_SIZE;
    int from = 1;

    while (true) {
      val url = getUrl(size, from);
      val result = read(url);
      val hits = getHits(result);

      for (val hit : hits) {
        val fileMeta = (ObjectNode) hit;
        fileMetas.add(fileMeta);
      }

      if (hits.size() < size) {
        break;
      }

      from += size;
    }
    return fileMetas.build();
  }

  @SneakyThrows
  private static JsonNode read(URL url) {
    return DEFAULT_MAPPER.readTree(url);
  }


}
