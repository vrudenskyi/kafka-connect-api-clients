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
package com.mckesson.kafka.connect.azure.client;

import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.mckesson.kafka.connect.azure.auth.AzureTokenClient;
import com.mckesson.kafka.connect.source.APIClientException;
import com.mckesson.kafka.connect.source.PollableAPIClient;
import com.mckesson.kafka.connect.utils.DateTimeUtils;
import com.mckesson.kafka.connect.utils.UrlBuilder;
import com.microsoft.aad.adal4j.AuthenticationResult;

/**
 * https://msdn.microsoft.com/library/azure/dn931934.aspx
 */
public class AzureActivityAPIClient implements PollableAPIClient {
  private static final String API_VERSION_DEFAULT = "2015-04-01";
  private static final Logger log = LoggerFactory.getLogger(AzureActivityAPIClient.class);

  private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'hh:mm:ss.SSSSSSS'Z'").withZone(ZoneOffset.UTC);
  private static final String BASE_ENDPOINT = "https://management.azure.com/subscriptions/{subscription-id}/providers/microsoft.insights/eventtypes/management/values?api-version={api-version}&$filter={filter-expression}";

  private String apiVersion = API_VERSION_DEFAULT;

  private final ObjectMapper jacksonObjectMapper = new ObjectMapper();

  private AzureTokenClient tokenClient;
  private HttpClient httpClient;
  private String subscriptionId;

  @Override
  public void configure(Map<String, ?> configs) {
    /*
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    this.apiEndpoint = config.getString(API_ENDPOINT_CONFIG);
    this.datatypes = config.getList(DATATYPES_CONFIG);
    this.pageSize = config.getInt(PAGE_SIZE_CONFIG);
    this.apiVersion = config.getString(API_VERSION_CONFIG);
    this.filterExpression = config.getString(FILTER_CONFIG);
    this.initialDays = config.getInt(INIT_DAYS_CONFIG);
    this.tokenClient = AzureTokenClient.configuredInstance(configs);
    */
  }

  public AzureActivityAPIClient apiVersion(String apiVersion) {
    this.apiVersion = StringUtils.isBlank(apiVersion) ? API_VERSION_DEFAULT : apiVersion;
    return this;
  }

  private AzureActivityAPIClient() {
  }

  public static AzureActivityAPIClient newInstance() {
    return new AzureActivityAPIClient();
  }

  public List<JsonNode> topEvents(int maxSize, LocalDateTime since) throws AzureClientException {

    LocalDateTime startDt = since;
    if (startDt == null || startDt.isBefore(LocalDateTime.now().minusDays(90))) {
      log.warn("Date {} is > 90 days. Use last 90 days", startDt);
      startDt = LocalDateTime.now().minusDays(89);
    }
    log.debug("Read from: {}", startDt);
    List<JsonNode> result = new ArrayList<>();

    AuthenticationResult token;
    try {
      token = tokenClient.getCurrentToken();
    } catch (Exception e) {
      throw new AzureClientException("Failed to obtain access token", e);
    }

    String filterExpression = String.format("eventTimestamp ge '%s'",
        DateTimeUtils.printDateTime(startDt, DATETIME_FORMATTER));

    String nextLink = new UrlBuilder(BASE_ENDPOINT)
        .routeParam("subscription-id", subscriptionId)
        .routeParam("api-version", apiVersion)
        .routeParam("filter-expression", filterExpression)
        .getUrl();

    while (StringUtils.isNotBlank(nextLink)) {
      HttpResponse resp;
      try {

        HttpGet req = new HttpGet(nextLink);
        req.addHeader(HttpHeaders.AUTHORIZATION, String.format("%s %s", token.getAccessTokenType(), token.getAccessToken()));
        req.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        resp = httpClient.execute(req);

      } catch (Exception e) {
        throw new AzureClientException("Failed to invoke query: " + nextLink, e);
      }

      JsonNode resultNode = readResponse(resp, JsonNode.class);
      JsonNode nlNode = resultNode.findValue("nextLink");
      JsonNode valueNode = resultNode.findValue("value");

      if (valueNode != null && valueNode.isArray()) {
        for (JsonNode n : valueNode) {
          String eventTs = n.get("eventTimestamp").asText();
          if (DateTimeUtils.parseDateTime(eventTs).isAfter(startDt)) {
            result.add(n);
            if (result.size() >= maxSize)
              return result;
          }

        }
      }
      nextLink = nlNode != null ? nlNode.asText() : null;
    }

    return result;

  }

  public Object subscriptions() throws AzureClientException {
    //checkInitialized();

    AuthenticationResult token;
    try {
      token = tokenClient.getCurrentToken();
    } catch (Exception e) {
      throw new AzureClientException("Failed to obtain access token", e);
    }

    String nextLink = new UrlBuilder("https://management.azure.com/subscriptions?api-version=2016-06-01").getUrl();
    HttpResponse resp;
    try {
      HttpGet req = new HttpGet(nextLink);
      req.addHeader(HttpHeaders.AUTHORIZATION, String.format("%s %s", token.getAccessTokenType(), token.getAccessToken()));
      req.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      resp = httpClient.execute(req);
    } catch (Exception e) {
      throw new AzureClientException("Failed to invoke query: " + nextLink, e);
    }

    return readResponse(resp, JsonNode.class);

  }

  private <T> T readResponse(HttpResponse resp, Class<T> valueType) throws AzureClientException {

    // check status
    int httpStatus = resp.getStatusLine().getStatusCode();
    if (HttpStatus.SC_OK == httpStatus) {
      try {
        T value = jacksonObjectMapper.readValue(resp.getEntity().getContent(), valueType);
        return value;
      } catch (Exception e) {
        throw new AzureClientException("Failed to read data from success request", httpStatus, e);
      }

    } else {

      log.warn("Not Ok http response for valueType: {}", valueType, httpStatus);
      String message = null;
      try {
        message = CharStreams.toString(new InputStreamReader(resp.getEntity().getContent(), Charsets.UTF_8));
        AzureErrorMessage errMessage = jacksonObjectMapper.readValue(message, AzureErrorMessage.class);
        throw new AzureClientException(errMessage.getMessage(), httpStatus);
      } catch (Exception e) {
        log.error("Failed to read error message from: {}", message);
        throw new AzureClientException("Failed to read error message: " + message, httpStatus, e);
      }

    }
  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Map<String, Object>> partitions() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

}
