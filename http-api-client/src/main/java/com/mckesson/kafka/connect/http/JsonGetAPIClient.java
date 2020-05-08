/**
 * Copyright  Vitalii Rudenskyi (vrudenskyi@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mckesson.kafka.connect.http;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.mckesson.kafka.connect.source.APIClientException;

import okhttp3.Response;
import okhttp3.Request.Builder;

public class JsonGetAPIClient extends HttpAPIClient {

  public static final String JSON_DATA_POINTER_CONFIG = "json.data.pointer";
  public static final String JSON_DATA_POINTER_DEFAULT = "/";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(JSON_DATA_POINTER_CONFIG, ConfigDef.Type.STRING, JSON_DATA_POINTER_DEFAULT, ConfigDef.Importance.MEDIUM, "URI of Jira server. Required.");

  private static final Logger log = LoggerFactory.getLogger(JsonGetAPIClient.class);

  private String dataPointer;

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.dataPointer = conf.getString(JSON_DATA_POINTER_CONFIG);
  }

  @Override
  protected Builder getRequestBuilder(Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll) {
    Builder builder = super.getRequestBuilder(partition, offset, itemsToPoll);
    builder.addHeader("Accept", "application/json");
    return builder;
  }

  @Override
  public List<Object> extractData(Map<String, Object> partition, Map<String, Object> offset, Response response) throws APIClientException {

    List<Object> data = Collections.emptyList();

    JsonNode node;
    try {
      log.debug("reading data from response...");
      node = mapper.readValue(response.body().byteStream(), JsonNode.class);
    } catch (Exception e1) {
      throw new APIClientException("failed to read response", e1);
    } finally {
      response.close();
    }

    JsonNode dataNode = node.at(dataPointer);
    if (dataNode.isMissingNode()) {
      log.warn("'{}'node is missing in response. Whole response to be returned", dataPointer);
      try {
        data = Arrays.asList(mapper.writeValueAsString(node));
      } catch (JsonProcessingException e) {
        data = Arrays.asList(node);
      }

    } else if (dataNode.isArray()) {
      log.debug("Extracting data from array node '{}' (size:{})", dataPointer, dataNode.size());
      data = new ArrayList<>(dataNode.size());
      for (JsonNode rec : dataNode) {
        try {
          data.add(mapper.writeValueAsString(rec));
        } catch (JsonProcessingException e) {
          data.add(rec);
        }
      }

    } else {
      log.warn("{} node is not an array, returned as singe object", dataPointer);
      try {
        data = Arrays.asList(mapper.writeValueAsString(dataNode));
      } catch (JsonProcessingException e) {
        data = Arrays.asList(dataNode);
      }
    }

    return data;

  }

}
