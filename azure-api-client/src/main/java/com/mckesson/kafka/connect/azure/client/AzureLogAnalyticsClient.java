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

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
 * 
 * Client for Azure Log Analytics REST API
 * @see https://dev.loganalytics.io/documentation/Using-the-API
 * 
 * 
 */
public class AzureLogAnalyticsClient implements PollableAPIClient {

  /* CONFIGS */
  public static final String API_VERSION_CONFIG = "azure.loganalytics.api.version";
  private static final String API_VERSION_DEFAULT = "v1";

  public static final String API_ENDPOINT_CONFIG = "azure.loganalytics.api.endpoint";
  public static final String API_ENDPOINT_DEFAULT = "https://api.loganalytics.io/{api-version}/workspaces/{workspace-id}/query";

  public static final String WORKSPACE_IDS_CONFIG = "azure.loganalytics.workspace.ids";

  public static final String QUERY_CONFIG = "azure.loganalytics.query";
  public static final String QUERY_DEFAULT = "SecurityEvent  | where {key-field} > datetime({offset}) {partitions-expression} | sort by {key-field} asc| limit {page-size}";

  public static final String QUERY_KEY_FIELD_CONFIG = "azure.loganalytics.query.keyField";
  public static final String QUERY_KEY_FIELD_DEFAULT = "TimeGenerated";

  public static final String QUERY_PARTITIONS_ENABLED_CONFIG = "azure.loganalytics.query.partitions.enabled";
  public static final Boolean QUERY_PARTITIONS_ENABLED_DEFAULT = Boolean.FALSE;

  public static final String QUERY_PARTITIONS_FIELD_CONFIG = "azure.loganalytics.query.partitions.field";
  public static final String QUERY_PARTITIONS_FIELD_DEFAULT = "Computer";

  public static final String QUERY_PARTITIONS_EXPRESSION_CONFIG = "azure.loganalytics.query.partitions.expression";
  public static final String QUERY_PARTITIONS_EXPRESSION_DEFAULT = "and {partitions-field} == \"{partitions-value}\"";

  public static final String QUERY_PARTITIONS_QUERY_CONFIG = "azure.loganalytics.query.partitions.query";
  public static final String QUERY_PARTITIONS_QUERY_DEFAULT = "SecurityEvent | summarize count() by {partitions-key}";

  public static final String QUERY_PARTITION_QUERY_VALUES_CONFIG = "azure.loganalytics.query.partitions.values";

  public static final String PAGE_SIZE_CONFIG = "azure.loganalytics.query.pageSize";
  public static final int PAGE_SIZE_DEFAULT = 5000;

  public static final String KEEP_EMPTY_VALUES_CONFIG = "azure.loganalytics.keepEmptyValues";
  public static final Boolean KEEP_EMPTY_VALUES_DEFAULT = Boolean.FALSE;

  

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(WORKSPACE_IDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.MEDIUM, "Workspaces to read to read. supports: directoryAudits, signIns ")
      .define(API_ENDPOINT_CONFIG, ConfigDef.Type.STRING, API_ENDPOINT_DEFAULT, ConfigDef.Importance.MEDIUM, "REST API endpoint. default: " + API_ENDPOINT_DEFAULT)
      .define(API_VERSION_CONFIG, ConfigDef.Type.STRING, API_VERSION_DEFAULT, ConfigDef.Importance.MEDIUM, "REST API Version. Default:" + API_VERSION_DEFAULT)
      .define(QUERY_CONFIG, ConfigDef.Type.STRING, QUERY_DEFAULT, ConfigDef.Importance.MEDIUM, "Query filter. default: " + QUERY_DEFAULT)
      .define(QUERY_KEY_FIELD_CONFIG, ConfigDef.Type.STRING, QUERY_KEY_FIELD_DEFAULT, ConfigDef.Importance.MEDIUM, "Name of {key-field}. default: " + QUERY_KEY_FIELD_DEFAULT)
      .define(PAGE_SIZE_CONFIG, ConfigDef.Type.INT, PAGE_SIZE_DEFAULT, ConfigDef.Importance.MEDIUM, "Page size  per API reqs. Default:" + PAGE_SIZE_DEFAULT)
      .define(KEEP_EMPTY_VALUES_CONFIG, ConfigDef.Type.BOOLEAN, KEEP_EMPTY_VALUES_DEFAULT, ConfigDef.Importance.LOW, "Keep empty values. Default:" + KEEP_EMPTY_VALUES_DEFAULT)
      .define(PollableAPIClient.INITIAL_OFFSET_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Initial offset for non-existing partition")
      ///for partitioning 
      .define(QUERY_PARTITIONS_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, QUERY_PARTITIONS_ENABLED_DEFAULT, ConfigDef.Importance.LOW, "Enable partitioning for query")
      .define(QUERY_PARTITIONS_FIELD_CONFIG, ConfigDef.Type.STRING, QUERY_PARTITIONS_FIELD_DEFAULT, ConfigDef.Importance.LOW, "Field to use for partitioning")
      .define(QUERY_PARTITIONS_EXPRESSION_CONFIG, ConfigDef.Type.STRING, QUERY_PARTITIONS_EXPRESSION_DEFAULT, ConfigDef.Importance.LOW, "where clause expression for partitioning")
      .define(QUERY_PARTITIONS_QUERY_CONFIG, ConfigDef.Type.STRING, QUERY_PARTITIONS_QUERY_DEFAULT, ConfigDef.Importance.LOW, "where clause expression for partitioning")
      .define(QUERY_PARTITION_QUERY_VALUES_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.LOW, "where clause expression for partitioning");

  public static final String OFFSET_KEY_TIME = "TimeGenerated";
  public static final String OFFSET_KEY_TIMESPAN = "timespan";

  public static final String PARTITION_WRKSPC_KEY = "WORKSPACE_ID";
  public static final String PARTITION_QUERY_KEY = "QUERY";

  private static final Logger log = LoggerFactory.getLogger(AzureLogAnalyticsClient.class);
  private final ObjectMapper jacksonObjectMapper = new ObjectMapper();

  //private static final String BASE_ENDPOINT = "https://graph.microsoft.com/{api-version}/auditLogs/directoryAudits?$top={page-size}&$filter={filter-expression}";

  private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC); 
      

  private ObjectMapper objectMapper = new ObjectMapper();

  private String initialOffset;
  private int pageSize;
  private String apiVersion;
  private List<String> workspaceIds;
  private String apiEndpoint;
  private String query;
  private String queryKey;
  private AzureTokenClient tokenClient;
  private HttpClient httpClient;
  private Boolean keepEmptyValues;

  private Boolean partitionsEnabled;
  private String partitionsField;
  private String partitionsExpr;
  private String partitionsQuery;
  private List<String> partitionsValues;

  @Override
  public void configure(Map<String, ?> configs) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    this.apiEndpoint = config.getString(API_ENDPOINT_CONFIG);
    this.workspaceIds = config.getList(WORKSPACE_IDS_CONFIG);
    this.pageSize = config.getInt(PAGE_SIZE_CONFIG);
    this.apiVersion = config.getString(API_VERSION_CONFIG);
    this.query = config.getString(QUERY_CONFIG);
    this.queryKey = config.getString(QUERY_KEY_FIELD_CONFIG);
    this.initialOffset = config.getString(PollableAPIClient.INITIAL_OFFSET_CONFIG);
    this.keepEmptyValues = config.getBoolean(KEEP_EMPTY_VALUES_CONFIG);
    this.partitionsEnabled = config.getBoolean(QUERY_PARTITIONS_ENABLED_CONFIG);
    this.partitionsField = config.getString(QUERY_PARTITIONS_FIELD_CONFIG);
    this.partitionsExpr = config.getString(QUERY_PARTITIONS_EXPRESSION_CONFIG);
    this.partitionsQuery = config.getString(QUERY_PARTITIONS_QUERY_CONFIG);
    this.partitionsValues = config.getList(QUERY_PARTITION_QUERY_VALUES_CONFIG);

    this.tokenClient = AzureTokenClient.configuredInstance(configs);
    this.httpClient = HttpClientConfig.buildClient(configs);
  }

  public AzureLogAnalyticsClient pageSize(int pageSize) {
    this.pageSize = pageSize;
    return this;
  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {
    if (partition == null)
      throw new NullPointerException("partition is null");
    if (offset == null)
      throw new NullPointerException("offset is null");

    if (offset.size() == 0) {
      offset.putAll(initialOffset(partition));
    }


    String timespan = null;
    String startDt = null;
    if (offset.containsKey(OFFSET_KEY_TIME)) {
      startDt = offset.get(OFFSET_KEY_TIME).toString();
    }
    if (offset.containsKey(OFFSET_KEY_TIMESPAN) && offset.get(OFFSET_KEY_TIMESPAN) != null) {
      timespan = offset.get(OFFSET_KEY_TIMESPAN).toString();
    }

    int retryCounter = 3;
    boolean done = false;
    List<SourceRecord> resultRecords = new ArrayList<>(itemsToPoll);
    while (!done) {
      if (stop != null && stop.get()) {
        log.debug("stop signal! exit where we are.");
        break;
      }

      Map<String, List<Map<String, Object>>> pollResult = new HashMap<>();
      LocalDateTime maxRowDt = DateTimeUtils.parseDateTime(startDt);
      String queryString = partition.get(PARTITION_QUERY_KEY)
          .toString()
          .replaceAll("\\{page-size\\}", "" + pageSize)
          .replaceAll("\\{offset\\}", offset.get(OFFSET_KEY_TIME).toString());

      JsonNode resultNode = null;
      try {
        resultNode = runQuery(partition.get(PARTITION_WRKSPC_KEY).toString(), queryString, timespan);
        retryCounter = 3;
      } catch (Exception e) {
        retryCounter--;
        if (retryCounter > 0 && e instanceof IOException) {
          log.debug("Failed query: {}\n error: {} \n will retry {} times more...", queryString, e.getMessage(), retryCounter);
          continue;
        } else {
          log.debug("Failed request, no more retries");
          throw new AzureClientException(e);
        }
      }

      JsonNode error = resultNode.findValue("error");
      if (error != null) {
        log.debug("Non-fatal API error: {} ", error.toString());
      }
      JsonNode tablesNode = resultNode.findValue("tables");

      if (tablesNode != null && tablesNode.isArray() && tablesNode.size() > 0) {
        log.trace("Read response from {} tables ", tablesNode.size());
        for (JsonNode tblNode : tablesNode) {
          String tblName = tblNode.get("name").asText();
          JsonNode cols = tblNode.get("columns");
          JsonNode rows = tblNode.get("rows");
          List<Map<String, Object>> tblData = new ArrayList<>(rows.size());
          if (rows != null && rows.isArray() && rows.size() > 0) {
            
            for (JsonNode row : rows) {
              Map<String, Object> rowData = new HashMap<>();
              for (int i = 0; i < cols.size(); i++) {
                String colName = cols.get(i).get("name").asText();
                String colType = cols.get(i).get("type").asText();
                Object colValue = readJsonValue(colName, colType, row.get(i));
                if (keepEmptyValues || (colValue != null && StringUtils.isNotBlank(colValue.toString()))) {
                  rowData.put(colName, colValue);
                }

                //key field
                if (this.queryKey.equals(colName) && row.get(i).asText() != null) {
                  LocalDateTime rowDt = DateTimeUtils.parseDateTime(row.get(i).asText());
                  if (rowDt.isAfter(maxRowDt)) {
                    maxRowDt = rowDt;
                  }
                }
              }
              tblData.add(rowData);
            }

          }
          pollResult.put(tblName, tblData);
        }

      } else {
        pollResult = Collections.emptyMap();
      }
      //remember maxTime  and next link in offset;
      offset.put(OFFSET_KEY_TIME, DateTimeUtils.printDateTime(maxRowDt, DATETIME_FORMATTER));
      //update or remove timespan
      long gap = ChronoUnit.DAYS.between(maxRowDt, LocalDateTime.now());
      if (gap > 0) {
        offset.put(OFFSET_KEY_TIMESPAN, "P" + gap + "D");
      } else {
        offset.remove(OFFSET_KEY_TIMESPAN);
      }
      List<SourceRecord> recsPage = createRecords(topic, partition, offset, pollResult);
      resultRecords.addAll(recsPage);
      done = recsPage == null || recsPage.size() == 0 || recsPage.size() < pageSize || resultRecords.size() >= itemsToPoll;

    }

    return resultRecords;

  }

  private Object readJsonValue(String colName, String colType, JsonNode valueNode) {
    if (valueNode == null || valueNode.isNull()) {
      return null;
    }
    Object retVal = null;
    switch (colType) {
      case "bool":
      case "boolean":
        retVal = valueNode.asBoolean();
        break;

      case "datetime":
      case "date":
        retVal = valueNode.asText();
        break;

      case "dynamic":
        try {
          retVal = objectMapper.writeValueAsString(valueNode);
        } catch (JsonProcessingException e) {
          valueNode.toString();
        }
        break;

      case "guid":
      case "uuid":
      case "uniqueid":
        retVal = valueNode.asText();
        break;

      case "int":
        retVal = valueNode.asInt();
        break;

      case "long":
        retVal = valueNode.asLong();
        break;

      case "real":
      case "double":
        retVal = valueNode.asDouble();
        break;

      case "timespan":
      case "time":
        retVal = valueNode.asText();
        break;

      case "string":
        retVal = valueNode.asText();
        break;

      default:
        retVal = valueNode.asText();
        break;
    }
    return retVal;
  }

  private List<SourceRecord> createRecords(String topic, Map<String, Object> partition, Map<String, Object> offset, Map<String, List<Map<String, Object>>> pollResult) {
    if (pollResult == null || pollResult.size() == 0) {
      return Collections.emptyList();
    }
    List<SourceRecord> records = new ArrayList<>();
    for (Map.Entry<String, List<Map<String, Object>>> e : pollResult.entrySet()) {
      for (Map<String, Object> r : e.getValue()) {
        try {
          records.add(new SourceRecord(partition, offset, topic, null, null, null, jacksonObjectMapper.writeValueAsString(r)));
        } catch (JsonProcessingException ex) {
          SourceRecord rec = new SourceRecord(partition, offset, topic, null, null, null, r.toString());
          rec.headers().addBoolean("failed", true);
          rec.headers().addString("errorMsg", ex.getMessage());
          records.add(rec);
        }
      }
    }

    return records;
  }

  private JsonNode runQuery(String workspaceId, String queryString, String timespan) throws Exception {

    log.trace("Run query: {},   \nworkspaceid:{}, \ntimespan:{}", queryString, workspaceId, timespan);
    String apiUrl = new UrlBuilder(apiEndpoint)
        .routeParam("api-version", apiVersion)
        .routeParam("workspace-id", workspaceId)
        .getUrl();

    AuthenticationResult token;
    try {
      token = tokenClient.getCurrentToken();
    } catch (Exception e) {
      throw new AzureClientException("Failed to obtain access token", e);
    }

    HttpPost req = new HttpPost(apiUrl);
    req.addHeader(HttpHeaders.AUTHORIZATION, String.format("%s %s", token.getAccessTokenType(), token.getAccessToken()));
    req.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
    
    

    ObjectNode body = objectMapper.createObjectNode();
    body.put("query", queryString);
    if (StringUtils.isNotBlank(timespan)) {
      body.put("timespan", timespan);
    }

    req.setEntity(new ByteArrayEntity(objectMapper.writeValueAsBytes(body)));
    HttpResponse resp = httpClient.execute(req);

    return readResponse(resp, JsonNode.class);

  }

  private <T> T readResponse(HttpResponse resp, Class<T> valueType) throws Exception {

    // check status
    int httpStatus = resp.getStatusLine().getStatusCode();
    if (HttpStatus.SC_OK == httpStatus) {
      T value = jacksonObjectMapper.readValue(resp.getEntity().getContent(), valueType);
      return value;
    } else {
      String message = null;
      message = CharStreams.toString(new InputStreamReader(resp.getEntity().getContent(), Charsets.UTF_8));
      GraphErrorMessage graphError = jacksonObjectMapper.readValue(message, GraphErrorMessage.class);
      throw new AzureClientException(graphError.getError().getMessage(), httpStatus);
    }
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {
    List<Map<String, Object>> partitions = new ArrayList<>(workspaceIds.size());

    log.debug("Create partitions  for wrkspcs: {} and {}={}", workspaceIds, QUERY_PARTITIONS_ENABLED_CONFIG, this.partitionsEnabled);
    for (String wsid : workspaceIds) {

      if (!partitionsEnabled) {
        Map<String, String> p = new HashMap<>(2);
        p.put(PARTITION_WRKSPC_KEY, wsid);
        String queryString = this.query
            .replaceAll("\\{key-field\\}", this.queryKey)
            .replaceAll("\\{partitions-expression\\}", "")
            .replaceAll("\\s{2,}", " ");
        p.put(PARTITION_QUERY_KEY, queryString);

        partitions.add(Collections.unmodifiableMap(p));
      } else {
        //build partitions experssions:
        Set<String> pValues = new HashSet<>();
        pValues.addAll(partitionsValues);
        if (StringUtils.isNoneBlank(partitionsField) && StringUtils.isNotBlank(partitionsQuery)) {
          String partQ = partitionsQuery.replaceAll("\\{partitions-key\\}", partitionsField);
          log.debug("run partitions query: {}", partQ);
          try {
            JsonNode partsNode = runQuery(wsid, partQ, null);
            JsonNode error = partsNode.findValue("error");
            if (error != null) {
              log.debug("Non-fatal API error: {} ", error.toString());
            }
            JsonNode tablesNode = partsNode.findValue("tables");
            if (tablesNode != null && tablesNode.isArray() && tablesNode.size() > 0) {
              log.trace("Read response from {} tables ", tablesNode.size());
              for (JsonNode tblNode : tablesNode) {
                JsonNode rows = tblNode.get("rows");
                JsonNode cols = tblNode.get("columns");
                int valueIdx = 0;
                int i = 0;
                for (JsonNode col : cols) {
                  if (partitionsField.equals(col.get("name").asText())) {
                    valueIdx = i;
                    break;
                  }
                  i++;
                }
                if (rows != null && rows.isArray() && rows.size() > 0) {
                  for (JsonNode row : rows) {
                    pValues.add(row.get(valueIdx).asText());
                  }
                }
              }
            }
          } catch (Exception e) {
            throw new APIClientException(e);
          }
        }

        for (String pv : pValues) {
          Map<String, Object> p = new HashMap<>(2);
          p.put(PARTITION_WRKSPC_KEY, wsid);
          String queryString = this.query
              .replaceAll("\\{key-field\\}", this.queryKey)
              .replaceAll("\\{partitions-expression\\}", this.partitionsExpr.replaceAll("\\{partitions-field\\}", this.partitionsField).replaceAll("\\{partitions-value\\}", pv))
              .replaceAll("\\s{2,}", " ");
          p.put(PARTITION_QUERY_KEY, queryString);
          partitions.add(p);
        }
      }
    }

    log.debug("Partitions created: {}", partitions);
    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {

    return new HashMap<String, Object>() {
      {
        String offset = StringUtils.isNotBlank(initialOffset) ? initialOffset : DateTimeUtils.printDateTime(LocalDateTime.now(), DATETIME_FORMATTER);
        put(OFFSET_KEY_TIME, offset);

        
        
        long daysDiff = ChronoUnit.DAYS.between(DateTimeUtils.parseDateTime(offset), LocalDateTime.now());
        if (daysDiff > 0) {
          put(OFFSET_KEY_TIMESPAN, "P" + daysDiff + "D");
        }

      }
    };

  }
  
  @Override
  public void close() {
    try {
      if (httpClient instanceof CloseableHttpClient) {
        ((CloseableHttpClient) httpClient).close();
      }
    } catch (Exception e) {
      log.error("Failed to close HttpClient", e);
    }
  }


}
