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
package com.vrudenskyi.kafka.connect.lastpass;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vrudensk.kafka.connect.utils.DateTimeUtils;
import com.vrudensk.kafka.connect.utils.OkHttpClientConfig;
import com.vrudenskyi.kafka.connect.source.APIClientException;
import com.vrudenskyi.kafka.connect.source.PollableAPIClient;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class LastPassAPIClient implements PollableAPIClient {

  private static final Logger log = LoggerFactory.getLogger(LastPassAPIClient.class);
  public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final DateTimeFormatter DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm:ss");

  /* CONFIGS */
  public static final String API_ENDPOINT_CONFIG = "lastpass.apiEndpoint";
  public static final String API_ENDPOINT_DEFAULT = "https://lastpass.com/enterpriseapi.php";

  public static final String CUSTOMER_ID_CONFIG = "lastpass.customerId";
  public static final String API_KEY_CONFIG = "lastpass.apiKey";

  public static final String INITIAL_OFFSET_CONFIG = "lastpass.initialOffset";
  public static final String INITIAL_OFFSET_DEFAULT = DateTimeUtils.printDateTime(LocalDateTime.now().minusDays(100), DT_FORMATTER);

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(API_ENDPOINT_CONFIG, ConfigDef.Type.STRING, API_ENDPOINT_DEFAULT, ConfigDef.Importance.HIGH, "URI of API endpoint. Default: " + API_ENDPOINT_DEFAULT)
      .define(CUSTOMER_ID_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "CustomerId. Required")
      .define(API_KEY_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "ApiKey. Required")
      .define(PollableAPIClient.INITIAL_OFFSET_CONFIG, ConfigDef.Type.STRING, INITIAL_OFFSET_DEFAULT, ConfigDef.Importance.LOW, "Initial offset");

  public static final String PARTITION_CUSTOMER_ID_KEY = "__CUSTOMER_ID";
  public static final String PARTITION_ENDPOINT_KEY = "__ENDPOINT";

  private final ObjectMapper mapper = new ObjectMapper();

  private static final String OFFSET_EVENT_TIME_KEY = "__EventTime";

  private OkHttpClient httpClient;
  private String endpoint;
  private String customerId;
  private String initialOffset;
  private Password apiKey;

  @Override
  public void configure(Map<String, ?> configs) {

    log.debug("Configuring LastPassAPIClient...");
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.customerId = conf.getString(CUSTOMER_ID_CONFIG);
    this.endpoint = conf.getString(API_ENDPOINT_CONFIG);
    this.apiKey = conf.getPassword(API_KEY_CONFIG);
    this.initialOffset = conf.getString(PollableAPIClient.INITIAL_OFFSET_CONFIG);

    try {
      this.httpClient = OkHttpClientConfig.buildClient(configs);
    } catch (Exception e) {
      throw new ConnectException("Failed to configure client", e);
    }
    log.debug("LastPassAPIClient configured.");

  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {

    log.debug("Poll lastpass for partition:{}, offset:{}", partition, offset);

    List<SourceRecord> allResult = new ArrayList<>();
    boolean done = false;

    while (!done) {

      String storedOffset = (String) offset.get(OFFSET_EVENT_TIME_KEY);
      //String newOffset = currentOffset;

      LocalDateTime offsetDate = DateTimeUtils.parseDateTime(storedOffset, DT_FORMATTER);
      LocalDateTime maxToDate = offsetDate.plusHours(12);
      LocalDateTime toDate = maxToDate.isBefore(LocalDateTime.now()) ? maxToDate : LocalDateTime.now();

      Map<String, Object> requestWrapper = new HashMap<>();
      requestWrapper.put("cid", this.customerId);
      requestWrapper.put("provhash", this.apiKey.value());
      requestWrapper.put("cmd", "reporting");
      Map<String, Object> requestData = new HashMap<>();
      requestData.put("format", "siem");
      requestData.put("from", DateTimeUtils.printDateTime(offsetDate.plusSeconds(1), DT_FORMATTER));
      requestData.put("to", DateTimeUtils.printDateTime(toDate, DT_FORMATTER));
      requestWrapper.put("data", requestData);

      log.debug("Perform request for data: {}", requestData);
      Request request;
      try {
        String requestBody = mapper.writeValueAsString(requestWrapper);
        log.trace("RequestBody: {}", requestBody);
        request = new Request.Builder()
            .url(this.endpoint)
            .post(RequestBody.create(JSON, requestBody))
            .build();
      } catch (JsonProcessingException e) {
        throw new APIClientException("Failed to create request", e);
      }

      List<SourceRecord> pollResult = new ArrayList<>();
      try {
        try (Response response = httpClient.newCall(request).execute()) {
          if (!response.isSuccessful()) {
            log.error("Unexpected code: {}  \n\t with body: {}\n", response, response.body().string());
            throw new APIClientException("Unexpected code: " + response);
          }

          log.debug("API call sucessfully finished. Extractind data");
          JsonNode node = mapper.readValue(response.body().byteStream(), JsonNode.class);
          JsonNode dataNode = node.at("/events");
          if (dataNode.isMissingNode()) {
            log.warn("/events node is missing in response");
            pollResult.add(buildRecord(topic, partition, offset, node));

          } else if (dataNode.isArray()) {
            log.debug("/events node size: {}", dataNode.size());
            if (dataNode.size() > 0) {
              LocalDateTime newOffsetDt = offsetDate;
              for (JsonNode rec : dataNode) {
                newOffsetDt = DateTimeUtils.max(DateTimeUtils.parseDateTime(rec.at("/Time").asText(), DT_FORMATTER), newOffsetDt);
                pollResult.add(buildRecord(topic, partition, offset, rec));
              }
              offset.put(OFFSET_EVENT_TIME_KEY, DateTimeUtils.printDateTime(newOffsetDt, DT_FORMATTER));
            }

          } else {
            log.warn("/events node is not an array");
            pollResult.add(buildRecord(topic, partition, offset, dataNode));
          }

          JsonNode nextNode = node.at("/next");
          if (!nextNode.isMissingNode()) {
            log.debug("/next node found in response. Read offset from {}", nextNode);
            offset.put(OFFSET_EVENT_TIME_KEY, DateTimeUtils.printMillis(nextNode.asLong(), DT_FORMATTER));
          }
        }

      } catch (IOException e) {
        throw new APIClientException("Failed http request" + request, e);
      }

      allResult.addAll(pollResult);
      done = pollResult.size() == 0 || allResult.size() >= itemsToPoll;
      if (pollResult.size() == 0 && maxToDate.isBefore(LocalDateTime.now())) {
        offset.put(OFFSET_EVENT_TIME_KEY, DateTimeUtils.printDateTime(maxToDate, DT_FORMATTER));
        log.debug("No data for the window. Switch to the next window: {}", DateTimeUtils.printDateTime(maxToDate, DT_FORMATTER));
        done = false;
      }
      pollResult.clear();

    }

    return allResult;
  }

  private SourceRecord buildRecord(String topic, Map<String, Object> partition, Map<String, Object> offset, JsonNode node) throws JsonProcessingException {
    SourceRecord r = new SourceRecord(partition, offset, topic, null, null, null, mapper.writeValueAsString(node));
    r.headers().addString("lastpass:customerId", this.customerId);
    return r;
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {

    List<Map<String, Object>> partitions = new ArrayList<>(1);
    Map<String, String> p = new HashMap<>(2);
    p.put(PARTITION_ENDPOINT_KEY, this.endpoint);
    p.put(PARTITION_CUSTOMER_ID_KEY, this.customerId);
    partitions.add(Collections.unmodifiableMap(p));

    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    Map<String, Object> offset = new HashMap<>(1);
    offset.put(OFFSET_EVENT_TIME_KEY, this.initialOffset);
    return offset;
  }

  @Override
  public void close() {
  }

}
