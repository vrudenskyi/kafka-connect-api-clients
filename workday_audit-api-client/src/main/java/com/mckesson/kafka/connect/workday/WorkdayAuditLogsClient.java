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
package com.mckesson.kafka.connect.workday;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.mckesson.kafka.connect.http.HttpAPIClient;
import com.mckesson.kafka.connect.source.APIClientException;
import com.mckesson.kafka.connect.source.PollableAPIClient;
import com.mckesson.kafka.connect.utils.DateTimeUtils;
import com.mckesson.kafka.connect.utils.UrlBuilder;

import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.Response;

/**
 * Client to retrieve activity log data from your Workday tenant. 
 */
public class WorkdayAuditLogsClient extends HttpAPIClient {

  private static final Logger log = LoggerFactory.getLogger(WorkdayAuditLogsClient.class);

  public static final String URL_CONFIG = "workday.url";
  public static final String TENANT_NAME_CONFIG = "workday.tenantName";
  public static final String USER_CONFIG = "workday.user";
  public static final String PASSWORD_CONFIG = "workday.password";
  public static final String REFRESH_TOKEN_CONFIG = "workday.refreshToken";
  public static final String API_VERISON_CONFIG = "workday.apiVersion";
  public static final String API_VERISON_DEFAULT = "v1";

  public static final String BATCH_SIZE_CONFIG = "workday.auditLogs.batchSize";
  public static final Integer BATCH_SIZE_DEFAULT = 100;

  public static final String QUERY_WINDOW_MINUTES_CONFIG = "workday.auditLogs.queryWindowMinutes";
  public static final int QUERY_WINDOW_MINUTES_DEFAULT = 10;

  public static final String SERVICE_ENDPOINT_TEMPLATE = "/ccx/api/{apiVersion}/{tenantName}/auditLogs";
  public static final String TOKEN_ENDPOINT_TEMPLATE = "/ccx/oauth2/{tenantName}/token";

  private static final DateTimeFormatter DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX").withZone(ZoneOffset.UTC);
  private static final DateTimeFormatter DT_FORMATTER_REC = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX").withZone(ZoneOffset.UTC);
  private static final String INITIAL_OFFSET_DEFAULT = DateTimeUtils.printDateTime(ZonedDateTime.now().minusDays(1), DT_FORMATTER);

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Workday URL. Required")
      .define(TENANT_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Tenant name. Required")
      .define(USER_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Username. Required")
      .define(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Basic auth password. Required")
      .define(REFRESH_TOKEN_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Basic auth password. Required")
      .define(QUERY_WINDOW_MINUTES_CONFIG, ConfigDef.Type.INT, QUERY_WINDOW_MINUTES_DEFAULT, ConfigDef.Importance.MEDIUM, "Query window minutes")
      .define(BATCH_SIZE_CONFIG, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, ConfigDef.Importance.MEDIUM, "Batch size")

      .define(API_VERISON_CONFIG, ConfigDef.Type.STRING, API_VERISON_DEFAULT, ConfigDef.Importance.LOW, "API version")
      .define(PollableAPIClient.INITIAL_OFFSET_CONFIG, ConfigDef.Type.STRING, INITIAL_OFFSET_DEFAULT, ConfigDef.Importance.LOW, "Initial offset");

  protected static final String OFFSET_FROM_KEY = "__from";
  protected static final String OFFSET_TO_KEY = "__to";
  protected static final String OFFSET_MAX_TIME_KEY = "__maxTime";
  protected static final String OFFSET_OFFSET_KEY = "__offset";

  protected String initialOffset;
  protected int queryWindow;
  protected int batchSize;

  private String tenantName;
  private String tenantUrl;
  private String apiVersion;

  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.initialOffset = conf.getString(PollableAPIClient.INITIAL_OFFSET_CONFIG);
    this.queryWindow = conf.getInt(QUERY_WINDOW_MINUTES_CONFIG);
    this.batchSize = conf.getInt(BATCH_SIZE_CONFIG);

    this.tenantUrl = conf.getString(WorkdayAuditLogsClient.URL_CONFIG);
    this.tenantName = conf.getString(WorkdayAuditLogsClient.TENANT_NAME_CONFIG);
    this.apiVersion = conf.getString(WorkdayAuditLogsClient.API_VERISON_CONFIG);

    String tenantEndpoint = new UrlBuilder(SERVICE_ENDPOINT_TEMPLATE)
        .routeParam("tenantName", this.tenantName)
        .routeParam("apiVersion", this.apiVersion)
        .getUrl();
    ((Map<String, Object>) configs).put(HttpAPIClient.SERVER_URI_CONFIG, this.tenantUrl);
    ((Map<String, Object>) configs).put(HttpAPIClient.ENDPPOINT_CONFIG, tenantEndpoint);
    ((Map<String, Object>) configs).put(HttpAPIClient.AUTH_TYPE_CONFIG, AuthType.CUSTOM.name());
    ((Map<String, Object>) configs).put(HttpAPIClient.AUTH_CLASS_CONFIG, WorkdayPreemptiveAuthenticator.class.getName());
    super.configure(configs);

  }

  @Override
  protected OkHttpClient.Builder httpClientBuilder(Map<String, ?> configs) throws Exception {
    OkHttpClient.Builder builder = super.httpClientBuilder(configs);
    builder.addInterceptor((Interceptor) builder.build().authenticator());
    return builder;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    Map<String, Object> offset = new HashMap<>(1);
    offset.put(OFFSET_FROM_KEY, initialOffset);
    return offset;
  }

  @Override
  protected Builder getRequestBuilder(Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll) {

    if (!offset.containsKey(OFFSET_TO_KEY)) {
      ZonedDateTime startTime = DateTimeUtils.parseZonedDateTime(offset.get(OFFSET_FROM_KEY).toString(), DT_FORMATTER);
      if (startTime.plusMinutes(queryWindow).compareTo(ZonedDateTime.now()) >= 0) {
        log.debug("skip request until: {}", startTime.plusMinutes(queryWindow));
        return null;
      }
      ZonedDateTime endTime = ObjectUtils.min(startTime.plusMinutes(queryWindow), ZonedDateTime.now(ZoneOffset.UTC));
      offset.put(OFFSET_TO_KEY, DateTimeUtils.printDateTime(endTime, DT_FORMATTER));
      log.debug("New query: {} => {}", offset.get(OFFSET_FROM_KEY), offset.get(OFFSET_TO_KEY));
    }

    Map<String, String> queryParams = new HashMap<>();
    queryParams.put("from", offset.get(OFFSET_FROM_KEY).toString());
    queryParams.put("to", offset.get(OFFSET_TO_KEY).toString());
    queryParams.put("offset", offset.getOrDefault(OFFSET_OFFSET_KEY, "0").toString());
    queryParams.put("limit", String.valueOf(this.batchSize));
    queryParams.put("type", "userActivity");

    Request.Builder requestBuilder = super.getRequestBuilder(partition, offset, itemsToPoll, null, queryParams)
        .addHeader("Accept", "application/json");

    return requestBuilder;

  }

  @Override
  public List<Object> extractData(Map<String, Object> partition, Map<String, Object> offset, Response response) throws APIClientException {

    List<Object> data = Collections.emptyList();
    ZonedDateTime maxTime = DateTimeUtils.parseZonedDateTime(offset.getOrDefault(OFFSET_MAX_TIME_KEY, offset.get(OFFSET_FROM_KEY)).toString(), DT_FORMATTER);

    JsonNode node;
    try {
      node = mapper.readValue(response.body().byteStream(), JsonNode.class);
    } catch (Exception e1) {
      throw new APIClientException("failed to read response", e1);
    } finally {
      response.close();
    }

    String dataPointer = "/data";
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
          ZonedDateTime recTime = DateTimeUtils.parseZonedDateTime(rec.at("/requestTime").asText(), DT_FORMATTER_REC);
          maxTime = ObjectUtils.max(recTime, maxTime);
        } catch (Exception e1) {
          //ignore 
        }

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

    int total = node.at("/total").asInt();
    long currentOffset = (long) offset.getOrDefault(OFFSET_OFFSET_KEY, 0L);
    if (total == 0) {
      offset.put(OFFSET_FROM_KEY, offset.get(OFFSET_TO_KEY));
      offset.put(OFFSET_OFFSET_KEY, 0L);
      offset.remove(OFFSET_TO_KEY);
      offset.remove(OFFSET_MAX_TIME_KEY);
      log.debug("no data for the window, updated offset: {}", offset);
    } else if (currentOffset + dataNode.size() >= total) {
      offset.put(OFFSET_FROM_KEY, DateTimeUtils.printDateTime(maxTime.plusSeconds(1), DT_FORMATTER));
      offset.put(OFFSET_OFFSET_KEY, 0L);
      offset.remove(OFFSET_TO_KEY);
      offset.remove(OFFSET_MAX_TIME_KEY);
      log.debug("window finished, updated offset: {}", offset);

    } else {
      offset.put(OFFSET_OFFSET_KEY, currentOffset + dataNode.size());
      offset.put(OFFSET_MAX_TIME_KEY, DateTimeUtils.printDateTime(maxTime, DT_FORMATTER));
      log.debug("continue query: {} => {} \n\toffset:{}/{} \n\tmaxTime:{}",
          offset.get(OFFSET_FROM_KEY),
          offset.get(OFFSET_TO_KEY),
          offset.get(OFFSET_OFFSET_KEY),
          total,
          offset.get(OFFSET_MAX_TIME_KEY));

    }
    return data;
  }

  @Override
  protected List<SourceRecord> createRecords(String topic, Map<String, Object> partition, Map<String, Object> offset, List<Object> dataList) {

    List<SourceRecord> result = new ArrayList<>(dataList.size());
    for (Object value : dataList) {
      SourceRecord r = new SourceRecord(partition, offset, topic, null, null, null, value);
      r.headers().addString(TENANT_NAME_CONFIG, this.tenantName);
      result.add(r);
    }

    return result;
  }

}
