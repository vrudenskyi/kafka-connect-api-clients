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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.mckesson.kafka.connect.azure.auth.AzureTokenClient;
import com.mckesson.kafka.connect.source.APIClientException;
import com.mckesson.kafka.connect.source.PollableAPIClient;
import com.mckesson.kafka.connect.utils.DateTimeUtils;
import com.mckesson.kafka.connect.utils.HttpClientConfig;
import com.mckesson.kafka.connect.utils.UrlBuilder;
import com.microsoft.aad.adal4j.AuthenticationResult;

/**
 * Useful resources 
 * https://docs.microsoft.com/en-us/azure/active-directory/active-directory-reporting-api-getting-started-azure-portal
 * https://docs.microsoft.com/en-us/azure/active-directory/active-directory-reporting-api-audit-samples
 * 
 *  
 * https://developer.microsoft.com/en-us/graph/docs/api-reference/beta/resources/directoryaudit
 */
public class AzureADAuditLogsClient implements PollableAPIClient {

  /* CONFIGS */

  public static final String DATATYPES_CONFIG = "azure.ad.auditLogs.datatypes";
  /**
   * key - type of data
   * value - field for filter
   */
  private static final Map<String, String> DATATYPE_TIME_COLUMNS = new HashMap<String, String>() {
    {
      put("directoryAudits", "activityDateTime");
      put("signIns", "createdDateTime");
    }
  };

  public static final String API_VERSION_CONFIG = "azure.ad.auditLogs.api.version";
  private static final String API_VERSION_DEFAULT = "beta";

  public static final String API_ENDPOINT_CONFIG = "azure.ad.auditLogs.api.endpoint";
  public static final String API_ENDPOINT_DEFAULT = "https://graph.microsoft.com/{api-version}/auditLogs/{item}?$top={page-size}&$filter={filter-expression}";

  public static final String PAGE_SIZE_CONFIG = "azure.ad.auditLogs.pageSize";
  public static final int PAGE_SIZE_DEFAULT = 500;

  public static final String FILTER_CONFIG = "azure.ad.auditLogs.filter";
  public static final String FILTER_DEFAULT = "{filter-key} gt {filter-value} and {filter-key} lt {filter-value2} ";

  public static final String INIT_DAYS_CONFIG = "azure.ad.auditLogs.initDays";

  public static final int INIT_DAYS_DEFAULT = 0;

  public static final String DATA_LATENCY_CONFIG = "azure.ad.auditLogs.data.latency";
  public static final Long DATA_LATENCY_DEFAULT = 600 * 1000L; //default 10 minutes

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(DATATYPES_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, "Datatypes to read. supports: directoryAudits, signIns ")
      .define(API_ENDPOINT_CONFIG, ConfigDef.Type.STRING, API_ENDPOINT_DEFAULT, ConfigDef.Importance.MEDIUM, "REST API endpoint. default: " + API_ENDPOINT_DEFAULT)
      .define(API_VERSION_CONFIG, ConfigDef.Type.STRING, API_VERSION_DEFAULT, ConfigDef.Importance.MEDIUM, "REST API Version. Default:" + API_VERSION_DEFAULT)
      .define(FILTER_CONFIG, ConfigDef.Type.STRING, FILTER_DEFAULT, ConfigDef.Importance.MEDIUM, "Query filter. default: " + FILTER_DEFAULT)
      .define(PAGE_SIZE_CONFIG, ConfigDef.Type.INT, PAGE_SIZE_DEFAULT, ConfigDef.Importance.MEDIUM, "Page size  per API reqs. Default:" + PAGE_SIZE_DEFAULT)
      .define(INIT_DAYS_CONFIG, ConfigDef.Type.INT, INIT_DAYS_DEFAULT, ConfigDef.Importance.MEDIUM, "Number of days to read behind on initial start. Default:" + INIT_DAYS_DEFAULT)
      .define(DATA_LATENCY_CONFIG, ConfigDef.Type.LONG, DATA_LATENCY_DEFAULT, ConfigDef.Importance.MEDIUM,
          "Latency data appears. see: https://docs.microsoft.com/en-us/azure/active-directory/active-directory-reporting-latencies-azure-portal. Default:" + DATA_LATENCY_DEFAULT)
      .define(INITIAL_OFFSET_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.LOW, "Initial offset");

  public static final String OFFSET_KEY_TIME = "activityDateTime";
  public static final String OFFSET_KEY_NEXTLINK = "nextLink";
  public static final String PARTITION_NAME_KEY = "DATA_TYPE";

  private static final Logger log = LoggerFactory.getLogger(AzureADAuditLogsClient.class);
  private final ObjectMapper jacksonObjectMapper = new ObjectMapper();

  //private static final String BASE_ENDPOINT = "https://graph.microsoft.com/{api-version}/auditLogs/directoryAudits?$top={page-size}&$filter={filter-expression}";

  private static final DateTimeFormatter OFFSET_DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS'Z'").withZone(ZoneOffset.UTC);

  protected AtomicBoolean stop;

  private int initialDays;
  private long dataLatency;
  private String initialOffset;
  private int pageSize;
  private String apiVersion;
  private List<String> datatypes;
  private String apiEndpoint;
  private String filterExpression;
  private AzureTokenClient tokenClient;
  private HttpClient httpClient;

  @Override
  public void configure(Map<String, ?> configs) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    this.apiEndpoint = config.getString(API_ENDPOINT_CONFIG);
    this.datatypes = config.getList(DATATYPES_CONFIG);
    this.pageSize = config.getInt(PAGE_SIZE_CONFIG);
    this.apiVersion = config.getString(API_VERSION_CONFIG);
    this.filterExpression = config.getString(FILTER_CONFIG);
    this.initialDays = config.getInt(INIT_DAYS_CONFIG);
    this.initialOffset = config.getString(PollableAPIClient.INITIAL_OFFSET_CONFIG);
    this.tokenClient = AzureTokenClient.configuredInstance(configs);
    this.httpClient = HttpClientConfig.buildClient(configs);
    this.dataLatency = config.getLong(DATA_LATENCY_CONFIG);

  }

  public AzureADAuditLogsClient pageSize(int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {
    if (partition == null)
      throw new NullPointerException("partition is null");
    if (offset == null)
      throw new NullPointerException("offset is null");
    this.stop = new AtomicBoolean(false);
    AuthenticationResult token;
    try {
      token = tokenClient.getCurrentToken();
    } catch (Exception e) {
      throw new AzureClientException("Failed to obtain access token", e);
    }

    String nextLink = null;

    if (offset.containsKey(OFFSET_KEY_NEXTLINK) && offset.get(OFFSET_KEY_NEXTLINK) != null) {
      nextLink = offset.get(OFFSET_KEY_NEXTLINK).toString();
    }

    String startDt = null;
    if (offset.containsKey(OFFSET_KEY_TIME)) {
      startDt = offset.get(OFFSET_KEY_TIME).toString();
    } else {
      log.info("Date {} is > {} days. Use last {} days", startDt, initialDays, initialDays);
      startDt = DateTimeUtils.printDateTime(LocalDateTime.now().minusDays(initialDays), OFFSET_DT_FORMATTER);
    }

    LocalDateTime maxTimeOffsetDt = DateTimeUtils.parseDateTime(startDt, OFFSET_DT_FORMATTER);

    if (StringUtils.isBlank(nextLink)) {
      String fe = this.filterExpression
          .replaceAll("\\{filter-key\\}", DATATYPE_TIME_COLUMNS.get(partition.get(PARTITION_NAME_KEY)))
          .replaceAll("\\{filter-value\\}", startDt)
          .replaceAll("\\{filter-value2\\}", DateTimeUtils.printDateTime(LocalDateTime.now(ZoneOffset.UTC).minusSeconds(dataLatency / 1000), OFFSET_DT_FORMATTER));

      log.debug("Build new query request for '{}' with expression: {}", partition.get(PARTITION_NAME_KEY), fe);
      nextLink = new UrlBuilder(apiEndpoint)
          .routeParam("page-size", "" + pageSize)
          .routeParam("item", partition.get(PARTITION_NAME_KEY).toString())
          .routeParam("api-version", apiVersion)
          .routeParam("filter-expression", fe).getUrl();

    } else {
      log.debug("Continue previous request: " + nextLink);
    }

    List<JsonNode> pollResult = new ArrayList<>();

    int retryCounter = 3;
    while (StringUtils.isNotBlank(nextLink)) {

      if (stop != null && stop.get()) {
        log.debug("stop signal! exit where we are.");
        break;
      }

      HttpResponse resp;
      try {
        HttpGet req = new HttpGet(nextLink);
        req.addHeader(HttpHeaders.AUTHORIZATION, String.format("%s %s", token.getAccessTokenType(), token.getAccessToken()));
        req.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        log.debug("Executing API req: {}", req.getURI());
        resp = httpClient.execute(req);
      } catch (Exception e) {
        throw new AzureClientException("Failed to invoke query: " + nextLink, e);
      }

      JsonNode resultNode = null;
      try {
        resultNode = readResponse(resp, JsonNode.class);
        retryCounter = 3;
      } catch (Exception e) {
        retryCounter--;
        if (retryCounter > 0) {
          log.debug("Failed request: {}\n error: {} \n will retry {} times more...", nextLink, e.getMessage(), retryCounter);
          continue;
        } else {
          log.debug("Failed request, no more retries");
          throw new AzureClientException(e);
        }
      } finally {
        EntityUtils.consumeQuietly(resp.getEntity());
      }

      JsonNode nlNode = resultNode.findValue("@odata.nextLink");
      nextLink = nlNode != null ? nlNode.asText() : null;

      JsonNode valueNode = resultNode.findValue("value");
      if (valueNode != null && valueNode.isArray() && valueNode.size() > 0) {
        log.debug("API response with value size: {} ", valueNode.size());
        for (JsonNode n : valueNode) {
          LocalDateTime eventDt = DateTimeUtils.parseDateTime(n.get(DATATYPE_TIME_COLUMNS.get(partition.get(PARTITION_NAME_KEY))).asText());
          if (eventDt.compareTo(maxTimeOffsetDt) > 0) {
            log.debug("offset switched: {} => {}", DateTimeUtils.printDateTime(maxTimeOffsetDt, OFFSET_DT_FORMATTER), DateTimeUtils.printDateTime(eventDt, OFFSET_DT_FORMATTER));
            maxTimeOffsetDt = eventDt;
          }
          pollResult.add(n);
        }
        if (pollResult.size() >= itemsToPoll) {
          //exit 
          break;
        }

      }
    }

    //remember maxTime  and next link in offset;
    offset.put(OFFSET_KEY_TIME, DateTimeUtils.printDateTime(maxTimeOffsetDt, OFFSET_DT_FORMATTER));
    if (nextLink != null) {
      offset.put(OFFSET_KEY_NEXTLINK, nextLink);
    } else {
      offset.remove(OFFSET_KEY_NEXTLINK);
    }

    return createRecords(topic, partition, offset, pollResult);

  }

  private List<SourceRecord> createRecords(String topic, Map<String, Object> partition, Map<String, Object> offset, List<JsonNode> pollResult) {
    if (pollResult == null || pollResult.size() == 0) {
      return Collections.emptyList();
    }
    List<SourceRecord> records = new ArrayList<>(pollResult.size());
    for (JsonNode n : pollResult) {
      try {
        SourceRecord rec = new SourceRecord(partition, offset, topic, null, null, null, jacksonObjectMapper.writeValueAsString(n));
        rec.headers().addString("aad.datatype", partition.get(PARTITION_NAME_KEY).toString());
        records.add(rec);
      } catch (JsonProcessingException e) {
        SourceRecord r = new SourceRecord(partition, offset, topic, null, null, null, n.toString());
        r.headers().addString("aad.datatype", partition.get(PARTITION_NAME_KEY).toString());
        r.headers().addBoolean("failed", true);
        r.headers().addString("errorMsg", e.getMessage());
        records.add(r);

      }
    }
    return records;
  }

  private <T> T readResponse(HttpResponse resp, Class<T> valueType) throws Exception {

    // check status
    int httpStatus = resp.getStatusLine().getStatusCode();
    log.debug("http response status: {}", httpStatus);

    if (HttpStatus.SC_OK == httpStatus) {
      try (InputStream respIS = resp.getEntity().getContent();) {
        T value = jacksonObjectMapper.readValue(respIS, valueType);
        return value;
      } catch (Exception e) {
        throw new APIClientException("Failed to read content from a success response", e);
      }

    } else if (429 == httpStatus) {
      String retryAfter = resp.getLastHeader("Retry-After").getValue();
      String rateLimitReason = resp.getLastHeader("Rate-Limit-Reason").getValue();

      String message;
      try (InputStream respIS = resp.getEntity().getContent();) {
        message = CharStreams.toString(new InputStreamReader(respIS, Charsets.UTF_8));
      } catch (Exception e) {
        throw new APIClientException("Failed to read content from a success response", e);
      } finally {
        EntityUtils.consumeQuietly(resp.getEntity());
      }
      log.error("API throttling error(429). Retry-After={},  Rate-Limit-Reason={} Message={}", retryAfter, rateLimitReason, message);
      GraphErrorMessage graphError = jacksonObjectMapper.readValue(message, GraphErrorMessage.class);
      log.debug("Sleep {} seconds before retry", retryAfter);
      sleep(Integer.valueOf(retryAfter) * 1000L);
      throw new Exception(graphError.getError().getMessage());
    } else {
      String message = null;
      message = CharStreams.toString(new InputStreamReader(resp.getEntity().getContent(), Charsets.UTF_8));
      GraphErrorMessage graphError = jacksonObjectMapper.readValue(message, GraphErrorMessage.class);
      throw new Exception(graphError.getError().getMessage());
    }

  }

  public List<Map<String, Object>> partitions() {
    List<Map<String, Object>> partitions = new ArrayList<>(datatypes.size());
    for (String item : datatypes) {
      partitions.add(Collections.singletonMap(PARTITION_NAME_KEY, item));
    }
    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) {
    return new HashMap<String, Object>() {
      {

        if (initialOffset != null) {
          put(OFFSET_KEY_TIME, initialOffset);
        } else {
          String startDt = DateTimeUtils.printDateTime(LocalDateTime.now().minusDays(initialDays), OFFSET_DT_FORMATTER);
          put(OFFSET_KEY_TIME, startDt);
        }

      }
    };
  }

  public void sleep(long millis) {
    long wakeuptime = System.currentTimeMillis() + millis;
    while (true) {
      try {
        Thread.sleep(1000L);
      } catch (InterruptedException e) {
        log.debug("Sleep interrupted");
      }
      if (System.currentTimeMillis() >= wakeuptime || stop.get())
        break;
    }
  }

  @Override
  public void close() {
    this.stop.set(true);
    try {
      if (httpClient instanceof CloseableHttpClient) {
        ((CloseableHttpClient) httpClient).close();
      }
    } catch (Exception e) {
      log.error("Failed to close HttpClient", e);
    }
  }

}
