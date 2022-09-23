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
package com.vrudenskyi.kafka.connect.o365;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.vrudensk.kafka.connect.utils.DateTimeUtils;
import com.vrudensk.kafka.connect.utils.UrlBuilder;
import com.vrudenskyi.kafka.connect.http.HttpAPIClient;
import com.vrudenskyi.kafka.connect.source.APIClientException;
import com.vrudenskyi.kafka.connect.source.PollableAPIClient;
import com.vrudenskyi.kafka.connect.source.TimeBasedPartitionHelper;

import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.Response;

/**
 *Client for <a href="https://docs.microsoft.com/en-us/previous-versions/office/developer/o365-enterprise-developers/jj984342(v=office.15)">Exchange reports</a> API
 *@see <a href="https://docs.microsoft.com/en-us/previous-versions/office/developer/o365-enterprise-developers/jj984335%28v%3doffice.15%29">MessageTrace report</a>
 */
public class O365ReportSvcPollableClient extends HttpAPIClient {

  private static final Logger log = LoggerFactory.getLogger(O365ReportSvcPollableClient.class);

  protected static final String OFFSET_START_DATE_KEY = "______startDate";
  protected static final String OFFSET_NEXT_URL_KEY = "______nextUrl";

  public static final String REPORT_TYPE_CONFIG = "o365.reporting.svc.reportType";

  public static final String REPORT_URI_CONFIG = "o365.reporting.svc.uri";
  public static final String REPORT_URI_DEFAULT = "/ecp/reportingwebservice/reporting.svc/{reportType}?$filter=StartDate eq datetime'{startDate}' and EndDate eq datetime'{endDate}'";

  public static final String INITIAL_START_DATE_DEFAULT = DateTimeUtils.printDateTime(ZonedDateTime.now(ZoneId.of("UTC")).minusDays(7));

  public static final String QUERY_WINDOW_MINUTES_CONFIG = "o365.reporting.svc.queryWindowMinutes";
  public static final int QUERY_WINDOW_MINUTES_DEFAULT = 60;

  public static final String DATA_DELAY_MINUTES_CONFIG = "o365.reporting.svc.dataDelayMinutes";
  public static final int DATA_DELAY_MINUTES_DEFAULT = 6 * 60; //6 hrs 

  public static final String BACKOFF_MINUTES_CONFIG = "o365.reporting.svc.backoffMinutes";
  public static final int BACKOFF_MINUTES_DEFAULT = 5; //

  public static final String NUMBER_OF_PARTITIONS_CONFIG = "o365.reporting.svc.numberOfPartitions";
  public static final int NUMBER_OF_PARTITIONS_DEFAULT = 1; //24hrs  

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(REPORT_TYPE_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
          "Report name. see: https://docs.microsoft.com/en-us/previous-versions/office/developer/o365-enterprise-developers/jj984342%28v%3doffice.15%29")
      .define(REPORT_URI_CONFIG, ConfigDef.Type.STRING, REPORT_URI_DEFAULT, ConfigDef.Importance.HIGH,
          "Uri for the report. default: /ecp/reportingwebservice/reporting.svc/{reportType}?$filter=StartDate eq datetime'{startDate}' and EndDate eq datetime'{endDate}'")
      .define(PollableAPIClient.INITIAL_OFFSET_CONFIG, ConfigDef.Type.STRING, INITIAL_START_DATE_DEFAULT, ConfigDef.Importance.LOW, "Initial startDate. Default: now() - 7 days")
      .define(QUERY_WINDOW_MINUTES_CONFIG, ConfigDef.Type.INT, QUERY_WINDOW_MINUTES_DEFAULT, ConfigDef.Importance.LOW, "Query window in munites.")
      .define(DATA_DELAY_MINUTES_CONFIG, ConfigDef.Type.INT, DATA_DELAY_MINUTES_DEFAULT, ConfigDef.Importance.LOW, "Time data can be delayed. ")
      .define(BACKOFF_MINUTES_CONFIG, ConfigDef.Type.INT, BACKOFF_MINUTES_DEFAULT, ConfigDef.Importance.LOW, "Backoff in minutes from now")
      .define(NUMBER_OF_PARTITIONS_CONFIG, ConfigDef.Type.INT, NUMBER_OF_PARTITIONS_DEFAULT, ConfigDef.Importance.LOW, "Number of partitions.");

  protected String initialStartDate;
  protected int queryWindow;
  protected int numberOfPartitions;
  protected int dataDelayMinutes;
  protected int backoffMinutes;

  protected static final String PARTITION_ID_KEY = "___partitionId";

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    initialStartDate = conf.getString(PollableAPIClient.INITIAL_OFFSET_CONFIG);
    queryWindow = conf.getInt(QUERY_WINDOW_MINUTES_CONFIG);
    dataDelayMinutes = conf.getInt(DATA_DELAY_MINUTES_CONFIG);
    backoffMinutes = conf.getInt(BACKOFF_MINUTES_CONFIG);
    numberOfPartitions = conf.getInt(NUMBER_OF_PARTITIONS_CONFIG);

    String reportType = conf.getString(REPORT_TYPE_CONFIG);
    String reportUri = conf.getString(REPORT_URI_CONFIG);

    ((Map<String, Object>) configs).put(HttpAPIClient.ENDPPOINT_CONFIG, new UrlBuilder(reportUri).routeParam("reportType", reportType).getUrl());
    super.configure(configs);

  }

  @Override
  protected Builder getRequestBuilder(Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll) {

    Request.Builder requestBuilder = new Request.Builder()
        .get()
        .addHeader("Accept", "application/json");

    if (offset.containsKey(OFFSET_NEXT_URL_KEY)) {
      requestBuilder.url(offset.get(OFFSET_NEXT_URL_KEY).toString());
      log.debug("NextLink request built for partitionId:{}  'nextLink': {}", partition.get(PARTITION_ID_KEY), offset.get(OFFSET_NEXT_URL_KEY).toString());
    } else {
      String startDate = offset.get(OFFSET_START_DATE_KEY).toString();
      int partitionId = Integer.valueOf((String) partition.getOrDefault(PARTITION_ID_KEY, "-1"));
      ZonedDateTime endDateTime = TimeBasedPartitionHelper.getEndDate(DateTimeUtils.parseZonedDateTime(startDate), this.queryWindow, partitionId, this.numberOfPartitions);
      ZonedDateTime edgeTime = ZonedDateTime.now(endDateTime.getZone()).minusMinutes(this.backoffMinutes);
      if (endDateTime.isAfter(edgeTime)) {
        //to make sure that data is already in-place
        log.debug("Skip request for partitionId:{} until {}. {} secs to wait", partition.get(PARTITION_ID_KEY), DateTimeUtils.printDateTime(edgeTime), ChronoUnit.SECONDS.between(edgeTime, endDateTime));
        return null;
      }
      String endDate = DateTimeUtils.printDateTime(endDateTime.minusNanos(1));
      UrlBuilder ub = new UrlBuilder(partition.get(PARTITION_URL_KEY).toString())
          .routeParam("startDate", startDate)
          .routeParam("endDate", endDate);
      requestBuilder.url(ub.getUrl());
      log.debug("New request built for partitionId:{} startDate:{}, endDate:{}", partition.get(PARTITION_ID_KEY), startDate, endDate);
    }

    return requestBuilder;

  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    Map<String, Object> offset = new HashMap<>(1);
    int partitionId = Integer.valueOf((String) partition.getOrDefault(PARTITION_ID_KEY, "-1"));
    ZonedDateTime startDateForPartition = TimeBasedPartitionHelper.initStartDate(DateTimeUtils.parseZonedDateTime(initialStartDate).truncatedTo(ChronoUnit.HOURS), partitionId, this.numberOfPartitions);
    offset.put(OFFSET_START_DATE_KEY, DateTimeUtils.printDateTime(startDateForPartition));
    log.debug("For partition: {} Initial Offset configured: {}", partition, offset);
    return offset;
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {
    List<Map<String, Object>> partitions;
    Map<String, Object> parentPartition = super.partitions().get(0);
    if (this.numberOfPartitions == 1) {
      partitions = Arrays.asList(parentPartition);
    } else {
      //extend partition created by parent
      partitions = new ArrayList<>(this.numberOfPartitions);
      for (int i = 0; i < this.numberOfPartitions; i++) {
        Map<String, Object> p = new HashMap<>(parentPartition);
        p.put(PARTITION_ID_KEY, String.valueOf(i));
        partitions.add(Collections.unmodifiableMap(p));
      }
    }

    log.debug("Configured {} partitions: {}", this.numberOfPartitions, partitions);
    return partitions;
  }

  @Override
  public List<Object> extractData(Map<String, Object> partition, Map<String, Object> offset, Response response) throws APIClientException {
    List<Object> data = Collections.emptyList();

    JsonNode node;
    try {
      node = mapper.readValue(response.body().byteStream(), JsonNode.class);
    } catch (Exception e) {
      throw new APIClientException("Failed to read response", e);
    } finally {
      response.close();
    }

    JsonNode dataNode = node.at("/value");
    if (dataNode.isMissingNode()) {
      log.warn("/value node is missing in response");
      try {
        data = Arrays.asList(mapper.writeValueAsString(node));
      } catch (JsonProcessingException e) {
        data = Arrays.asList(node);
      }

    } else if (dataNode.isArray()) {
      data = new ArrayList<>(dataNode.size());
      for (JsonNode rec : dataNode) {
        try {
          data.add(mapper.writeValueAsString(rec));
        } catch (JsonProcessingException e) {
          data.add(rec);
        }
      }

    } else {
      log.warn("/value node is not an array");
      try {
        data = Arrays.asList(mapper.writeValueAsString(dataNode));
      } catch (JsonProcessingException e) {
        data = Arrays.asList(dataNode);
      }
    }

    JsonNode nextLinkNode = node.at("/odata.nextLink");
    if (nextLinkNode.isValueNode()) {
      String nextLink = nextLinkNode.asText();
      //idiots 
      if (nextLink.startsWith("../../")) {
        nextLink = serverUri + "/ecp/" + nextLink.substring(6);
      }
      offset.put(OFFSET_NEXT_URL_KEY, nextLink);
      log.debug("offset nextUrl updated for partitionId:{}, new value: {}", partition.get(PARTITION_ID_KEY), nextLink);
    } else if (data.size() > 0) {
      //roll date only if there is some data, if no data start date will remain unchanged
      int partitionId = Integer.valueOf((String) partition.getOrDefault(PARTITION_ID_KEY, "-1"));
      String startDate = offset.get(OFFSET_START_DATE_KEY).toString();
      String newStartDate = DateTimeUtils.printDateTime(TimeBasedPartitionHelper.shiftStartDate(DateTimeUtils.parseZonedDateTime(startDate), this.queryWindow, partitionId, this.numberOfPartitions));
      log.debug("offset startDate shifted for partitionId:{}, new value: {} => {}", partitionId, startDate, newStartDate);
      offset.remove(OFFSET_NEXT_URL_KEY);
      offset.put(OFFSET_START_DATE_KEY, newStartDate);
    } else {
      //no data: if consuming 'from the past' - increment, if 'near now' - keep the same
      ZonedDateTime maxTimeForNoData = ZonedDateTime.now(ZoneId.of("UTC")).minusMinutes(dataDelayMinutes);
      ZonedDateTime currentStartDate = DateTimeUtils.parseZonedDateTime(offset.get(OFFSET_START_DATE_KEY).toString());

      if (currentStartDate.isBefore(maxTimeForNoData)) {
        int partitionId = Integer.valueOf((String) partition.getOrDefault(PARTITION_ID_KEY, "-1"));
        ZonedDateTime newStartTime = TimeBasedPartitionHelper.shiftStartDate(currentStartDate, this.queryWindow, partitionId, this.numberOfPartitions);
        log.debug("offset updated for no-data case partitionId:{}, new value: {} => {}", partition.get(PARTITION_ID_KEY), offset.get(OFFSET_START_DATE_KEY), DateTimeUtils.printDateTime(newStartTime));
        offset.put(OFFSET_START_DATE_KEY, DateTimeUtils.printDateTime(newStartTime));

      } else {
        log.debug("offset not-changed  for no-data case partitionId:{}, new value: {}", partition.get(PARTITION_ID_KEY), offset.get(OFFSET_START_DATE_KEY));
      }
      offset.remove(OFFSET_NEXT_URL_KEY);
    }

    return data;
  }

}
