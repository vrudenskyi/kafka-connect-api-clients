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
package com.mckesson.kafka.connect.salesforce;

import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.force.api.http.Http;
import com.force.api.http.HttpRequest;
import com.force.api.http.HttpResponse;
import com.mckesson.kafka.connect.source.APIClientException;
import com.mckesson.kafka.connect.source.PollableSourceConfig;

public class EventLogPollableAPIClient extends SalesforceAPIClient {

  private static final Logger log = LoggerFactory.getLogger(EventLogPollableAPIClient.class);

  public static final String SFDC_EVENTLOG_TYPES_CONFIG = "sfdc.eventlog.types";

  public static final ConfigDef CONFIG_DEF = PollableSourceConfig.addConfig(
      new ConfigDef()
          .define(SFDC_EVENTLOG_TYPES_CONFIG, Type.LIST, null, Importance.MEDIUM, "Event types to read"));

  protected static final String HEADER_EVENTLOG_TYPE = "sfdc.eventlog.type";

  protected static final String PARTITION_EVENT_TYPE_KEY = "_EventLogFile_EventType";

  private static final String OFFSET_LOG_TIME_KEY = "_EventLogFile_LastModifiedDate";
  private static final String EVENTLOG_QUERY_DEFAULT = "select Id, EventType, LastModifiedDate, LogFileLength, LogFile, Interval, LogDate, Sequence from EventLogFile where EventType='{eventType}' and LastModifiedDate > {offset} order by LastModifiedDate LIMIT 1";

  private List<String> eventTypes;

  @Override
  public void configure(Map<String, ?> configs) {
    log.debug("Configuring SFDC Eventlog SFDC Client");
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    eventTypes = conf.getList(SFDC_EVENTLOG_TYPES_CONFIG);
    log.debug("Configured EventTypes: {}", eventTypes);
    super.configure(configs);
  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {
    String soql = EVENTLOG_QUERY_DEFAULT
        .replaceAll("\\{offset\\}", offset.get(OFFSET_LOG_TIME_KEY).toString())
        .replaceAll("\\{eventType\\}", partition.get(PARTITION_EVENT_TYPE_KEY).toString());
    List<Map> qr = executeQuery(soql, offset, Map.class);

    if (qr.size() == 0) {
      log.debug("No new records from EventLogFile.eventType={}", partition.get(PARTITION_EVENT_TYPE_KEY));
      return Collections.emptyList();
    }

    List<SourceRecord> pollResult;
    Map fileRow = qr.get(0);
    log.debug("row found from EventLogFile.EventType={}: {}", partition.get(PARTITION_EVENT_TYPE_KEY), fileRow);
    String newOffset = (String) fileRow.get("LastModifiedDate");
    String logFile = (String) fileRow.get("LogFile");
    HttpRequest req = new HttpRequest()
        .url(forceApi.getSession().getApiEndpoint() + logFile)
        .method("GET");
    req.setAuthorization("Bearer " + forceApi.getSession().getAccessToken());
    req.setRequestTimeout(apiConfig.getRequestTimeout());

    Map<String, Object> fileMeta = new HashMap<>();
    fileMeta.put("fileMeta.Id", fileRow.get("Id"));
    fileMeta.put("fileMeta.LogFile", fileRow.get("LogFile"));
    fileMeta.put("fileMeta.EventType", fileRow.get("EventType"));
    fileMeta.put("fileMeta.LogFileLength", fileRow.get("LogFileLength"));
    fileMeta.put("fileMeta.LogDate", fileRow.get("LogDate"));
    fileMeta.put("fileMeta.Interval", fileRow.get("Interval"));
    fileMeta.put("fileMeta.Sequence", fileRow.get("Sequence"));

    log.debug("retrieving data from logFile:{}", logFile);
    HttpResponse resp = Http.send(req);
    //TODO: implement temporary file if  LogFileLength is long
    try (
        Reader reader = new InputStreamReader(resp.getStream());
        CSVParser parser = new CSVParser(reader, CSVFormat.EXCEL.withHeader());) {
      List<CSVRecord> csvRecs = parser.getRecords();
      log.debug("retieved records: {}", csvRecs.size());
      pollResult = new ArrayList<>(csvRecs.size());

      for (final CSVRecord r : csvRecs) {
        try {
          Map rData = r.toMap();
          rData.putAll(fileMeta);
          SourceRecord rec = new SourceRecord(partition, offset, topic, null, null, null, jacksonObjectMapper.writeValueAsString(rData));
          rec.headers().addString(HEADER_EVENTLOG_TYPE, partition.get(PARTITION_EVENT_TYPE_KEY).toString());
          pollResult.add(rec);

        } catch (JsonProcessingException ex) {
          SourceRecord failedRec = new SourceRecord(partition, offset, topic, null, null, null, r.toString());
          failedRec.headers().addBoolean("failed", true);
          failedRec.headers().addString("errorMsg", ex.getMessage());
          pollResult.add(failedRec);
        }
      }
    } catch (Exception e) {
      throw new APIClientException("Failed to parse CSV from: " + logFile, e);
    }
    log.debug("offset for {} set to: {}", partition.get(PARTITION_EVENT_TYPE_KEY), newOffset);
    offset.put(OFFSET_LOG_TIME_KEY, newOffset);
    return pollResult;
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {
    if (eventTypes == null) {
      log.debug("Eventypes are not explicitly configured. Will be retrieved from saleforce");
      String soql = "select EventType from EventLogFile group by EventType";
      List<Map> qr = executeQuery(soql, null, Map.class);
      eventTypes = new ArrayList<>(qr.size());
      qr.forEach(e -> {
        eventTypes.add(e.get("EventType").toString());
      });

    }
    List<Map<String, Object>> partitions = new ArrayList<>(eventTypes.size());
    eventTypes.forEach(e -> {
      partitions.add(Collections.singletonMap(PARTITION_EVENT_TYPE_KEY, e));
    });
    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    Map<String, Object> offset = new HashMap<>(1);
    offset.put(OFFSET_LOG_TIME_KEY, this.initialOffset);
    return offset;
  }

}
