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
package com.vrudenskyi.kafka.connect.jira;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
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

import okhttp3.Credentials;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class JiraAPIClient implements PollableAPIClient {

  private static final Logger log = LoggerFactory.getLogger(JiraAPIClient.class);
  private static final DateTimeFormatter RECORD_DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXXX");
  private static final DateTimeFormatter JQL_DT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd' 'HH:mm");

  public static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");

  /* CONFIGS */
  public static final String SERVER_URI_CONFIG = "jira.serverUri";
  public static final String USER_CONFIG = "jira.user";
  public static final String PASSWORD_CONFIG = "jira.password";

  public static final String API_VERSION_CONFIG = "jira.apiVersion";
  public static final String API_VERSION_DEFAULT = "latest";

  public static final String SEARCH_FIELDS_CONFIG = "jira.searchFields";
  public static final List<String> SEARCH_FIELDS_DEFAULT = Collections.emptyList();

  public static final String EXPAND_FIELDS_CONFIG = "jira.expandFields";
  public static final List<String> EXPAND_FIELDS_DEFAULT = Collections.emptyList();

  public static final String SEARCH_PAGE_SIZE_CONFIG = "jira.searchPageSize";
  public static final Integer SEARCH_PAGE_SIZE_DEFAULT = 200;

  public static final String REMOVE_NULL_FIELDS_CONFIG = "jira.removeNullFields";
  public static final Boolean REMOVE_NULL_FIELDS_DEFAULT = Boolean.TRUE;

  public static final String JQL_CONFIG = "jira.jql";
  public static final String PROJECTS_CONFIG = "jira.projects";

  
  public static final String INITIAL_OFFSET_DEFAULT = "1970-01-01T00:00:00.000Z";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(SERVER_URI_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "URI of Jira server. Required.")
      .define(USER_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Jira username. Required")
      .define(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Jira password. Required")
      .define(SEARCH_FIELDS_CONFIG, ConfigDef.Type.LIST, SEARCH_FIELDS_DEFAULT, ConfigDef.Importance.MEDIUM, "Search fields")
      .define(EXPAND_FIELDS_CONFIG, ConfigDef.Type.LIST, EXPAND_FIELDS_DEFAULT, ConfigDef.Importance.MEDIUM, "Expand fields")

      .define(JQL_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "JQL")
      .define(PROJECTS_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.HIGH, "List of projects to pull data from with default JQL")
      .define(API_VERSION_CONFIG, ConfigDef.Type.STRING, API_VERSION_DEFAULT, ConfigDef.Importance.HIGH, "Jira API version. Default: " + API_VERSION_DEFAULT)

      .define(SEARCH_PAGE_SIZE_CONFIG, ConfigDef.Type.INT, SEARCH_PAGE_SIZE_DEFAULT, ConfigDef.Importance.LOW, "Search pageSize. default:" + SEARCH_PAGE_SIZE_DEFAULT)
      .define(REMOVE_NULL_FIELDS_CONFIG, ConfigDef.Type.BOOLEAN, REMOVE_NULL_FIELDS_DEFAULT, ConfigDef.Importance.LOW, "Remove fields will null values. Default: " + REMOVE_NULL_FIELDS_DEFAULT)
      .define(PollableAPIClient.INITIAL_OFFSET_CONFIG, ConfigDef.Type.STRING, INITIAL_OFFSET_DEFAULT, ConfigDef.Importance.LOW, "Initial offset");

  //public static final String PARTITION_SEARCH_TYPE_KEY = "__SEARCH_TYPE";
  public static final String PARTITION_JQL_KEY = "__JQL";
  public static final String PARTITION_SERVER_URI_KEY = "__SERVER_URI";
  public static final String DEFAULT_JQL = "(project = {project}) AND updatedDate > '{offset}' ORDER BY updatedDate ASC";

  private final ObjectMapper mapper = new ObjectMapper();

  private static final String OFFSET_VALUE_KEY = "__UpdateTime";
  private static final String OFFSET_PAGED_START_AT_KEY = "__StartAt";
  private static final String OFFSET_PAGED_KEY = "__UpdateTimePaged";

  private OkHttpClient httpClient;
  private String initialOffset;
  private Set<String> searchFields;
  private Set<String> expandFields;
  private Integer searchPageSize;

  private String jql;
  private List<String> projects;
  private TimeZone jqlTimezone;

  private String apiVersion;
  private String offsetAt = "/fields/updated";
  private Boolean removeNullFields;
  private String serverUri;
  private String username;
  private Password passwd;

  @Override
  public void configure(Map<String, ?> configs) {

    log.debug("Configuring JiraAPIClient...");
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.username = conf.getString(USER_CONFIG);
    this.serverUri = conf.getString(SERVER_URI_CONFIG);
    this.passwd = conf.getPassword(PASSWORD_CONFIG);
    this.initialOffset = conf.getString(PollableAPIClient.INITIAL_OFFSET_CONFIG);
    List<String> fieldsList = conf.getList(SEARCH_FIELDS_CONFIG);
    this.searchFields = fieldsList == null ? null : new HashSet<>(fieldsList);
    this.searchPageSize = conf.getInt(SEARCH_PAGE_SIZE_CONFIG);
    this.jql = conf.getString(JQL_CONFIG);
    this.projects = conf.getList(PROJECTS_CONFIG);
    this.apiVersion = conf.getString(API_VERSION_CONFIG);
    this.removeNullFields = conf.getBoolean(REMOVE_NULL_FIELDS_CONFIG);

    if (StringUtils.isBlank(this.jql) && (this.projects == null || this.projects.size() == 0)) {
      log.error("Jira configuration is wrong. Either jql or projects need to be defined");
      throw new ConnectException("Jira configuration is wrong. Either jql or projects need to be defined");
    }

    try {
      this.httpClient = OkHttpClientConfig.buildClient(configs);
    } catch (Exception e) {
      throw new ConnectException("Failed to configure client", e);
    }

    log.debug("Retrieve jira Timezome for user: {}", this.username);
    //see https://community.atlassian.com/t5/Jira-questions/JIRA-API-JQL-datetime-format-does-not-have-timezone/qaq-p/870844
    Request request = new Request.Builder()
        .url(this.serverUri + "/rest/api/" + this.apiVersion + "/myself")
        .addHeader("Authorization", Credentials.basic(this.username, this.passwd.value()))
        .get()
        .build();

    try {
      try (Response response = httpClient.newCall(request).execute()) {
        if (!response.isSuccessful()) {
          log.error("Unexpected code: {}  \n\t with body: {}\n", response, response.body().string());
          throw new APIClientException("Unexpected code: " + response);
        }
        JsonNode node = mapper.readValue(response.body().byteStream(), JsonNode.class);
        String timeZone = node.at("/timeZone").asText();
        this.jqlTimezone = TimeZone.getTimeZone(timeZone);
        log.debug("user: {}  has jqlTimezone:{}", this.username, this.jqlTimezone);
      }
    } catch (Exception e) {
      throw new ConnectException(e);
    }
    log.info("JiraAPIClient configured.");
  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {

    ZonedDateTime savedOffset = DateTimeUtils.parseZonedDateTime((String) offset.getOrDefault(OFFSET_VALUE_KEY, initialOffset), RECORD_DT_FORMATTER);
    //savedOffset in user's timezone
    String offsetForQuery = DateTimeUtils.printDateTime(savedOffset.withZoneSameInstant(jqlTimezone.toZoneId()), JQL_DT_FORMATTER);

    Map<String, Object> requestData = new HashMap<>();
    requestData.put("jql", partition.get(PARTITION_JQL_KEY).toString().replaceAll("\\{offset\\}", offsetForQuery));
    requestData.put("maxResults", Math.min(searchPageSize, itemsToPoll));
    requestData.put("validateQuery", true);
    if (searchFields.size() > 0) {
      requestData.put("fields", searchFields);
    }

    boolean done = false;
    List<SourceRecord> pollResult = new ArrayList<>();
    while (!done) {

      Integer startAt = Integer.valueOf(offset.getOrDefault(OFFSET_PAGED_START_AT_KEY, "0").toString());
      requestData.put("startAt", startAt);
      Request request;
      log.debug("Search jira: {} ", requestData);
      try {
        String requestBody = mapper.writeValueAsString(requestData);
        log.trace("RequestBody: {}", requestBody);
        request = new Request.Builder()
            .url(this.serverUri + "/rest/api/" + this.apiVersion + "/search")
            .addHeader("Authorization", Credentials.basic(this.username, this.passwd.value()))
            .post(RequestBody.create(JSON, requestBody))
            .build();
      } catch (JsonProcessingException e) {
        throw new APIClientException("Failed to create request", e);
      }
      try {
        try (Response response = httpClient.newCall(request).execute()) {
          if (!response.isSuccessful()) {
            log.error("Unexpected code: {}  \n\t with body: {}\n", response, response.body().string());
            throw new APIClientException("Unexpected code: " + response);
          }
          JsonNode node = mapper.readValue(response.body().byteStream(), JsonNode.class);
          JsonNode dataNode = node.at("/issues");
          if (dataNode.isMissingNode()) {
            log.warn("/issues node is missing in response");
            pollResult.add(buildRecord(topic, partition, offset, node, false));

          } else if (dataNode.isArray()) {
            log.debug("/issues node size: {}", dataNode.size());
            String pagedOffsetStr = (String) offset.get(OFFSET_PAGED_KEY);
            ZonedDateTime maxPagedOffset = pagedOffsetStr != null ? DateTimeUtils.parseZonedDateTime(pagedOffsetStr, RECORD_DT_FORMATTER) : savedOffset;
            if (dataNode.size() > 0) {
              for (JsonNode rec : dataNode) {

                maxPagedOffset = DateTimeUtils.max(DateTimeUtils.parseZonedDateTime(rec.at(this.offsetAt).asText(), RECORD_DT_FORMATTER), maxPagedOffset);
                pollResult.add(buildRecord(topic, partition, offset, rec, removeNullFields));
              }
              offset.put(OFFSET_PAGED_KEY, DateTimeUtils.printDateTime(maxPagedOffset, RECORD_DT_FORMATTER));
              offset.put(OFFSET_PAGED_START_AT_KEY, Integer.valueOf(startAt + dataNode.size()).toString());
            }
          } else {
            log.warn("/issues node is not an array");
            pollResult.add(buildRecord(topic, partition, offset, dataNode, false));
          }

          //update offset depending on continious search or not
          boolean hasMore = (startAt + dataNode.size()) < node.at("/total").asInt();
          if (!hasMore) {
            if (offset.containsKey(OFFSET_PAGED_KEY)) {
              //update offset to maxPagedOffset + 1 minute
              offset.put(OFFSET_VALUE_KEY, DateTimeUtils.printDateTime(DateTimeUtils.parseZonedDateTime((String) offset.get(OFFSET_PAGED_KEY), RECORD_DT_FORMATTER).plusMinutes(1), RECORD_DT_FORMATTER));
            }
            offset.remove(OFFSET_PAGED_START_AT_KEY);
            offset.remove(OFFSET_PAGED_KEY);
          }

          done = !hasMore || pollResult.size() >= itemsToPoll;

        }
      } catch (IOException e) {
        throw new APIClientException("Failed http request" + request, e);
      }
    }

    return pollResult;

  }

  private SourceRecord buildRecord(String topic, Map<String, Object> partition, Map<String, Object> offset, JsonNode data, boolean removeEmptyFields) throws JsonProcessingException {

    if (this.removeNullFields) {
      Iterator<JsonNode> i = data.at("/fields").iterator();
      while (i.hasNext()) {
        JsonNode n = i.next();
        if (n.isNull()) {
          i.remove();
        }
      }
    }
    SourceRecord r = new SourceRecord(partition, offset, topic, null, null, null, mapper.writeValueAsString(data));
    r.headers().addString("jira.source", this.serverUri);
    r.headers().addString("jira.project", data.at("/fields/project/key").asText(""));
    return r;
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {

    List<Map<String, Object>> partitions = new ArrayList<>();
    if (StringUtils.isNotBlank(this.jql)) {
      Map<String, String> p = new HashMap<>(2);
      p.put(PARTITION_SERVER_URI_KEY, this.serverUri);
      p.put(PARTITION_JQL_KEY, this.jql);
      partitions.add(Collections.unmodifiableMap(p));
    } else if (this.projects != null && this.projects.size() > 0) {
      for (String pName : projects) {
        Map<String, String> p = new HashMap<>(2);
        p.put(PARTITION_SERVER_URI_KEY, this.serverUri);
        p.put(PARTITION_JQL_KEY, DEFAULT_JQL.replaceAll("\\{project\\}", pName));
        partitions.add(Collections.unmodifiableMap(p));
      }
    } else {
      throw new APIClientException("Not properly configured either jql or projects must be defined");
    }

    log.debug("Configured partitions: {}", partitions);
    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    Map<String, Object> offset = new HashMap<>(3);
    offset.put(OFFSET_VALUE_KEY, this.initialOffset);
    return offset;
  }

  @Override
  public void close() {

  }

}
