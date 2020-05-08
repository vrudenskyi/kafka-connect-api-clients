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
package com.mckesson.kafka.connect.splunk;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mckesson.kafka.connect.source.APIClientException;
import com.mckesson.kafka.connect.source.PollableAPIClient;
import com.mckesson.kafka.connect.source.PollableSourceConfig;
import com.mckesson.kafka.connect.utils.DateTimeUtils;
import com.splunk.Args;
import com.splunk.Event;
import com.splunk.Job;
import com.splunk.ResultsReader;
import com.splunk.ResultsReaderCsv;
import com.splunk.ResultsReaderJson;
import com.splunk.ResultsReaderXml;
import com.splunk.SSLSecurityProtocol;
import com.splunk.SavedSearch;
import com.splunk.SavedSearchCollection;
import com.splunk.SavedSearchCollectionArgs;
import com.splunk.SavedSearchDispatchArgs;
import com.splunk.Service;

public class SplunkPollableAPIClient implements PollableAPIClient {

  private static final Logger log = LoggerFactory.getLogger(SplunkPollableAPIClient.class);

  public static final String SPLUNK_HOST_CONFIG = "splunk.host";
  public static final String SPLUNK_PORT_CONFIG = "splunk.port";
  public static final Integer SPLUNK_PORT_DEFAULT = 8089;

  public static final String SPLUNK_USER_CONFIG = "splunk.user";
  public static final String SPLUNK_PASSWORD_CONFIG = "splunk.password";

  public static final String SPLUNK_SEARCHES_CONFIG = "splunk.searches";

  public static final String SPLUNK_SEARCH_NAME_CONFIG = "name";

  public static final String SPLUNK_SEARCH_SCHEDULED_CONFIG = "scheduled";
  public static final Boolean SPLUNK_SEARCH_SCHEDULED_DEFAULT = Boolean.FALSE;

  public static final String SPLUNK_SEARCH_LATEST_TIME_CONFIG = "latestTime";
  public static final String SPLUNK_SEARCH_LATEST_TIME_DEFAULT = "now";

  public static final String SPLUNK_SEARCH_OUTPUT_MODE_CONFIG = "outputMode";
  public static final String SPLUNK_SEARCH_OUTPUT_MODE_DEFAULT = "json";

  public static final String SPLUNK_SEARCH_USE_SAVED_TIME_CONFIG = "useSavedTime";
  public static final Boolean SPLUNK_SEARCH_USE_SAVED_TIME_DEFAULT = Boolean.FALSE;

  public static final String SPLUNK_SEARCH_APP_CONFIG = "app";
  public static final String SPLUNK_SEARCH_APP_DEFAULT = "search";

  public static final ConfigDef CONFIG_DEF = PollableSourceConfig.addConfig(
      new ConfigDef()
          .define(SPLUNK_HOST_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Splunk host")
          .define(SPLUNK_PORT_CONFIG, Type.INT, SPLUNK_PORT_DEFAULT, Importance.HIGH, "Splunk port. Default: 8089")
          .define(SPLUNK_USER_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "splunk  user")
          .define(SPLUNK_PASSWORD_CONFIG, Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Splunk password")
          .define(SPLUNK_SEARCHES_CONFIG, Type.LIST, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Splunk search name aliases"));

  public static final ConfigDef SEARCH_CONFIG_DEF = new ConfigDef()
      .define(SPLUNK_SEARCH_NAME_CONFIG, Type.STRING, null, Importance.HIGH, "Search name")
      .define(SPLUNK_SEARCH_APP_CONFIG, Type.STRING, SPLUNK_SEARCH_APP_DEFAULT, Importance.HIGH, "Search app")
      .define(SPLUNK_SEARCH_SCHEDULED_CONFIG, Type.BOOLEAN, SPLUNK_SEARCH_SCHEDULED_DEFAULT, Importance.MEDIUM, "Is Scheduled search")
      .define(SPLUNK_SEARCH_LATEST_TIME_CONFIG, Type.STRING, SPLUNK_SEARCH_LATEST_TIME_DEFAULT, Importance.MEDIUM, "Latest time. Default: now")
      .define(PollableAPIClient.INITIAL_OFFSET_CONFIG, Type.STRING, null, Importance.MEDIUM, "Default initial time")
      .define(SPLUNK_SEARCH_OUTPUT_MODE_CONFIG, Type.STRING, SPLUNK_SEARCH_OUTPUT_MODE_DEFAULT, Importance.MEDIUM, "Output Mode")
      .define(SPLUNK_SEARCH_USE_SAVED_TIME_CONFIG, Type.BOOLEAN, SPLUNK_SEARCH_USE_SAVED_TIME_DEFAULT, Importance.MEDIUM, "Use time saved in search");

  public static final String PARTITION_SEARCH_CONF_KEY = "_searchConf";
  public static final String PARTITION_SEARCH_NAME_KEY = "_searchName";
  public static final String PARTITION_SEARCH_APP_KEY = "_searchApp";
  public static final String PARTITION_SEARCH_SCHEDULED_KEY = "_searchScheduled";

  public static final String OFFSET_SEARCH_TIME_KEY = "_searchTime";

  public static final String OFFSET_JOB_SID_KEY = "_job_SID";
  public static final String OFFSET_JOB_OFFSET_KEY = "_job_offset";
  public static final String OFFSET_JOB_RESULT_COUNT_KEY = "_job_resultCount";

  public static final String SPLUNK_SEARCH_NAME_HEADER = "splunk.searchName";
  public static final String SPLUNK_SOURCE_HEADER = "splunk.source";

  private final ObjectMapper jacksonObjectMapper = new ObjectMapper();

  private String host;
  private Integer port;
  private String username;
  private Password password;

  private Map<String, SimpleConfig> searchConfigs;

  private Service service;

  @Override
  public void configure(Map<String, ?> configs) {
    // TODO Auto-generated method stub

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.host = conf.getString(SPLUNK_HOST_CONFIG);
    this.port = conf.getInt(SPLUNK_PORT_CONFIG);
    this.username = conf.getString(SPLUNK_USER_CONFIG);
    this.password = conf.getPassword(SPLUNK_PASSWORD_CONFIG);
    List<String> searchAliases = conf.getList(SPLUNK_SEARCHES_CONFIG);
    searchConfigs = new HashMap<>(searchAliases.size());
    for (String alias : searchAliases) {
      searchConfigs.put(alias, new SimpleConfig(SEARCH_CONFIG_DEF, conf.originalsWithPrefix(SPLUNK_SEARCHES_CONFIG + "." + alias + ".")));
    }
  }

  private Service getService() {
    if (service == null) {
      Map<String, Object> conf = new HashMap<>();
      conf.put("host", host);
      conf.put("port", port);
      conf.put("username", username);
      conf.put("password", password.value());
      Service.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);
      service = Service.connect(conf);
    }
    return service;
  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {

    SimpleConfig searchConf = searchConfigs.get(partition.get(PARTITION_SEARCH_CONF_KEY));

    String searchName = (String) partition.get(PARTITION_SEARCH_NAME_KEY);
    Job job;
    if (offset.containsKey(OFFSET_JOB_SID_KEY) && offset.get(OFFSET_JOB_SID_KEY) != null) {
      log.info("Resume reading from JOB_SID: {}", offset.get(OFFSET_JOB_SID_KEY));
      job = service.getJob((String) offset.get(OFFSET_JOB_SID_KEY));
    } else if ((Boolean) partition.get(PARTITION_SEARCH_SCHEDULED_KEY)) {
      log.info("Scheduled search {}, check for job result afer time: {}", searchName, offset.get(OFFSET_SEARCH_TIME_KEY));
      job = findJob(searchName,
          (String) partition.get(PARTITION_SEARCH_APP_KEY),
          (String) offset.get(OFFSET_SEARCH_TIME_KEY),
          stop);
      if (job == null) {
        log.info("No new jobs was found for search {}. Exit poll.", searchName);
        return Collections.emptyList();
      }

      log.debug("Scheduled job found: Sid={}, latestDate={}", job.getSid(), job.getLatestTime());
      offset.put(OFFSET_JOB_SID_KEY, job.getSid());
      offset.put(OFFSET_SEARCH_TIME_KEY, DateTimeUtils.printDate(job.getLatestTime()));
      offset.put(OFFSET_JOB_RESULT_COUNT_KEY, job.getResultCount());
      offset.put(OFFSET_JOB_OFFSET_KEY, 0);

    } else {
      String searchFrom = offset.get(OFFSET_SEARCH_TIME_KEY).toString();
      log.info("Run new job for search {},  for Time: {}", searchName, searchFrom);

      job = runSearch(searchName,
          (String) partition.get(PARTITION_SEARCH_APP_KEY),
          searchConf.getBoolean(SPLUNK_SEARCH_USE_SAVED_TIME_CONFIG),
          searchFrom,
          searchConf.getString(SPLUNK_SEARCH_LATEST_TIME_CONFIG),
          stop);
      //exit if stop 
      if (stop.get())
        return Collections.emptyList();

      offset.put(OFFSET_JOB_SID_KEY, job.getSid());
      offset.put(OFFSET_SEARCH_TIME_KEY, DateTimeUtils.printDate(job.getLatestTime()));
      offset.put(OFFSET_JOB_RESULT_COUNT_KEY, job.getResultCount());
      offset.put(OFFSET_JOB_OFFSET_KEY, 0);
    }

    List<Event> events = readJobResults(job, offset, itemsToPoll, searchConf.getString(SPLUNK_SEARCH_OUTPUT_MODE_CONFIG), stop);
    //exit if stop 
    if (stop.get())
      return Collections.emptyList();

    List<SourceRecord> recs = new ArrayList<>(events.size());
    events.forEach(e -> {
      recs.add(toRecord(topic, partition, offset, e));
    });
    return recs;

  }

  private Job findJob(String searchName, String appName, String searchTime, AtomicBoolean stop) throws APIClientException {

    log.debug("Find job for: name={}, app={}, time={}", searchName, appName, searchTime);
    SavedSearchCollectionArgs searchArgs = new SavedSearchCollectionArgs();
    searchArgs.setSearch(searchName);
    searchArgs.setApp(appName);
    SavedSearchCollection searches = getService().getSavedSearches(searchArgs);

    SavedSearch search = searches.get(searchName);
    if (search == null) {
      throw new APIClientException("Search not found: " + searchName);
    }

    List<Job> jobs = Arrays.asList(search.history());
    log.debug("Found {} jobs for {}", jobs.size(), searchName);
    jobs.forEach(j -> {
      log.debug("JOB: {}, Date:{}", j.getSid(), j.getLatestTime());
    });

    Collections.sort(jobs, new Comparator<Job>() {
      @Override
      public int compare(Job j1, Job j2) {
        return j2.getLatestTime().compareTo(j1.getLatestTime());
      }
    });
    Date searchDate = StringUtils.isNotBlank(searchTime) ? DateTimeUtils.parseDate(searchTime) : Date.from(Instant.now().minus(1, ChronoUnit.DAYS));
    for (Job job : jobs) {
      if (job.isDone() && job.getLatestTime().after(searchDate)) {
        return job;
      }
    }

    log.debug("No matching job found.");
    return null;
  }

  private SourceRecord toRecord(String topic, Map<String, Object> partition, Map<String, Object> offset, Event evt) {

    SourceRecord rec;
    try {
      rec = new SourceRecord(partition, offset, topic, null, jacksonObjectMapper.writeValueAsString(evt));
      rec.headers().addString(SPLUNK_SEARCH_NAME_HEADER, partition.get(PARTITION_SEARCH_NAME_KEY).toString());
      rec.headers().addString(SPLUNK_SOURCE_HEADER, host + ":" + port);
    } catch (JsonProcessingException ex) {
      log.warn("Failed to write Json as String: " + evt, ex);
      rec = new SourceRecord(partition, offset, topic, null, evt.toString());
    }
    return rec;

  }

  private Job runSearch(String searchName, String appName, Boolean savedTime, String earliestTime, String latestTime, AtomicBoolean stopFlag) throws APIClientException {
    try {
      log.debug("Get saved searches: name={}, app={}", searchName, appName);
      SavedSearchCollectionArgs searchArgs = new SavedSearchCollectionArgs();
      searchArgs.setSearch(searchName);
      searchArgs.setApp(appName);
      SavedSearchCollection searches = getService().getSavedSearches(searchArgs);

      SavedSearch search = searches.get(searchName);
      if (search == null) {
        throw new APIClientException("Search not found: " + searchName);
      }

      SavedSearchDispatchArgs args = null;
      if (!savedTime) {
        args = new SavedSearchDispatchArgs();
        args.setDispatchEarliestTime(earliestTime);
        args.setDispatchLatestTime(latestTime);
      }

      Job job = search.dispatch(args);
      log.debug("Dispatched... waiting for result...");
      while (!job.isDone()) {
        // If no outputs are available, optionally print status
        Thread.sleep(1000L);
        if (stopFlag.get()) {
          log.debug("Stop signal received. Terminate search and exit");
          return null;
        }
      }
      if (job.isReady()) {
        int scanned = job.getScanCount();
        int matched = job.getEventCount();
        int results = job.getResultCount();
        float runDuration = job.getRunDuration();
        log.debug("Job for search '{}' is done. runDuration={}, scanned={}, matched={}, resultCount={}", searchName, runDuration, scanned, matched, results);
      }
      return job;
    } catch (Exception e) {
      throw new APIClientException("Search failed", e);
    }
  }

  private List<Event> readJobResults(Job job, Map<String, Object> offset, int itemsToPoll, String outputMode, AtomicBoolean stopFlag) throws APIClientException {
    Args outputArgs = new Args();
    Integer offsetCount = (Integer) offset.get(OFFSET_JOB_OFFSET_KEY);
    outputArgs.put("output_mode", outputMode.toLowerCase());
    outputArgs.put("count", itemsToPoll);
    outputArgs.put("offset", offsetCount);

    List<Event> result = new ArrayList<Event>(itemsToPoll);
    try {
      InputStream stream = job.getResults(outputArgs);
      if (stopFlag.get()) {
        log.info("Stop signal received. Terminate job reading and exit");
        return null;
      }

      ResultsReader resultsReader;
      switch (outputMode.toLowerCase()) {
        case "json":
          resultsReader = new ResultsReaderJson(stream);
          break;

        case "xml":
          resultsReader = new ResultsReaderXml(stream);
          break;

        case "csv":
          resultsReader = new ResultsReaderCsv(stream);
          break;

        default:
          resultsReader = new ResultsReaderJson(stream);
      }

      if (stopFlag.get()) {
        log.info("Stop signal received. Terminate job reading and exit");
        return null;
      }

      resultsReader.forEach(result::add);
    } catch (IOException e) {
      throw new APIClientException("Reading JobResults failed", e);
    }

    if (result.size() >= itemsToPoll) {
      offsetCount += result.size();
      log.info("Job not finished yet. New job offset: {}", offsetCount);
      offset.put(OFFSET_JOB_OFFSET_KEY, offsetCount);
    } else {
      log.info("Job finished. Total result: {},  Meta.resultCount: {}", offsetCount + result.size(), job.getResultCount());
      offset.remove(OFFSET_JOB_SID_KEY);
      offset.remove(OFFSET_JOB_OFFSET_KEY);
      offset.remove(OFFSET_JOB_RESULT_COUNT_KEY);

    }

    return result;
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {
    List<Map<String, Object>> partitions = new ArrayList<>(searchConfigs.size());

    for (Map.Entry<String, SimpleConfig> conf : searchConfigs.entrySet()) {
      String sName = conf.getValue().getString(SPLUNK_SEARCH_NAME_CONFIG);

      HashMap<String, Object> partition = new HashMap<String, Object>() {

        {
          put(PARTITION_SEARCH_CONF_KEY, conf.getKey());
          put(PARTITION_SEARCH_NAME_KEY, StringUtils.isNotBlank(sName) ? sName : conf.getKey());
          put(PARTITION_SEARCH_APP_KEY, conf.getValue().getString(SplunkPollableAPIClient.SPLUNK_SEARCH_APP_CONFIG));
          put(PARTITION_SEARCH_SCHEDULED_KEY, conf.getValue().getBoolean(SplunkPollableAPIClient.SPLUNK_SEARCH_SCHEDULED_CONFIG));
        }
      };

      partitions.add(Collections.unmodifiableMap(partition));
    }
    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    String initialTime = searchConfigs.get(partition.get(PARTITION_SEARCH_CONF_KEY)).getString(PollableAPIClient.INITIAL_OFFSET_CONFIG);
    return new HashMap<String, Object>() {
      {
        put(OFFSET_SEARCH_TIME_KEY, StringUtils.isNotBlank(initialTime) ? initialTime : DateTimeUtils.printDateTime(LocalDateTime.now().minusDays(1)));
      }
    };
  }

  @Override
  public void close() {
    service = null;
  }

}
