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
package com.mckesson.kafka.connect.gcscc;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountJwtAccessCredentials;
import com.google.cloud.securitycenter.v1.ListFindingsRequest;
import com.google.cloud.securitycenter.v1.ListFindingsResponse.ListFindingsResult;
import com.google.cloud.securitycenter.v1.ListSourcesRequest;
import com.google.cloud.securitycenter.v1.OrganizationName;
import com.google.cloud.securitycenter.v1.SecurityCenterClient;
import com.google.cloud.securitycenter.v1.SecurityCenterClient.ListFindingsPagedResponse;
import com.google.cloud.securitycenter.v1.SecurityCenterClient.ListSourcesPagedResponse;
import com.google.cloud.securitycenter.v1.SecurityCenterSettings;
import com.google.cloud.securitycenter.v1.Source;
import com.google.cloud.securitycenter.v1.SourceName;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;
import com.google.protobuf.util.Timestamps;
import com.mckesson.kafka.connect.source.APIClientException;
import com.mckesson.kafka.connect.source.PollableAPIClient;


/**
 * https://cloud.google.com/security-command-center/docs/how-to-api-list-findings
 */
public class FindingsAPIClient implements PollableAPIClient {

  private static final Logger log = LoggerFactory.getLogger(FindingsAPIClient.class);
  private static final Printer JSON_PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();

  public static final String GCP_CREDENTIALS_FILE_PATH_CONFIG = "gcp.credentials.file.path";
  public static final String GCP_CREDENTIALS_JSON_CONFIG = "gcp.credentials.json";

  public static final String GCSCC_ORG_ID_CONFIG = "gcscc.organization.id";
  public static final String GCSCC_SOURCE_IDS_CONFIG = "gcscc.sources";

  public static final String GCSCC_API_FILTER_CONFIG = "gcscc.filter";
  public static final String GCSCC_API_FILTER_DEFAULT = "state = \"ACTIVE\" AND event_time > \"{offset}\"";

  public static final String GCSCC_API_PAGE_SIZE_CONFIG = "gcscc.pageSize";
  public static final Integer GCSCC_API_PAGE_SIZE_DEFAULT = 100;

  public static final String GCSCC_API_ORDER_BY_CONFIG = "gcscc.orderBy";
  public static final String GCSCC_API_ORDER_BY_DEFAULT = "event_time";

  public static final String GCSCC_API_COMPARE_DURATION_CONFIG = "gcscc.compareDuration";

  public static final String INITIAL_OFFSET_DEFAULT = Timestamps.toString(Timestamps.EPOCH);

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(GCSCC_ORG_ID_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Organization ID")
      .define(GCSCC_SOURCE_IDS_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.HIGH, "List of source IDs for organization. Default: will retrive all using API")
      .define(GCP_CREDENTIALS_FILE_PATH_CONFIG, Type.STRING, null, Importance.HIGH, "The path to the GCP credentials file")
      .define(GCP_CREDENTIALS_JSON_CONFIG, Type.STRING, null, Importance.HIGH, "GCP JSON credentials")
      .define(INITIAL_OFFSET_CONFIG, Type.STRING, INITIAL_OFFSET_DEFAULT, Importance.HIGH, "Initial event_time")
      .define(GCSCC_API_FILTER_CONFIG, Type.STRING, GCSCC_API_FILTER_DEFAULT, Importance.MEDIUM, "filter expression, default:" + GCSCC_API_FILTER_DEFAULT)
      .define(GCSCC_API_PAGE_SIZE_CONFIG, Type.INT, GCSCC_API_PAGE_SIZE_DEFAULT, Importance.LOW, "API page size, default:" + GCSCC_API_PAGE_SIZE_DEFAULT)
      .define(GCSCC_API_COMPARE_DURATION_CONFIG, Type.STRING, "", Importance.LOW, "API compareDuration")
      .define(GCSCC_API_ORDER_BY_CONFIG, Type.STRING, GCSCC_API_ORDER_BY_DEFAULT, Importance.MEDIUM, "API sortBy string, default:" + GCSCC_API_ORDER_BY_DEFAULT);

  private static final String PARTITION_PARENT_KEY = "_P_PARENT";

  private static final String OFFSET_EVENT_TIME_KEY = "_EVENT_TIME";
  private static final String OFFSET_NEXT_PAGE_TOKEN_KEY = "_NEXT_PAGE_TOKEN";
  private static final String OFFSET_MAX_PAGE_TIME_KEY = "_MAX_PAGE_TIME";

  private String orgId;
  private List<String> sourceIds;
  private String initialOffset;
  private int pageSize;
  private String orderBy;
  private String filter;
  private Duration compareDuration;

  private SecurityCenterClient sccClient;

  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);

    this.orgId = conf.getString(GCSCC_ORG_ID_CONFIG);
    this.sourceIds = conf.getList(GCSCC_SOURCE_IDS_CONFIG);
    this.initialOffset = conf.getString(INITIAL_OFFSET_CONFIG);
    this.pageSize = conf.getInt(GCSCC_API_PAGE_SIZE_CONFIG);
    this.orderBy = conf.getString(GCSCC_API_ORDER_BY_CONFIG);
    this.filter = conf.getString(GCSCC_API_FILTER_CONFIG);
    String compareDurationString = conf.getString(GCSCC_API_COMPARE_DURATION_CONFIG);
    if (StringUtils.isNotBlank(compareDurationString)) {
      this.compareDuration = Durations.fromMillis(java.time.Duration.parse(compareDurationString).toMillis());
    }

    try {
      InputStream credsIS;
      String credsJson = conf.getString(GCP_CREDENTIALS_JSON_CONFIG);
      String credsFile = conf.getString(GCP_CREDENTIALS_FILE_PATH_CONFIG);
      if (StringUtils.isNotBlank(credsJson)) {
        credsIS = new ByteArrayInputStream(credsJson.getBytes());
      } else if (StringUtils.isNotBlank(credsFile)) {
        credsIS = new FileInputStream(credsFile);
      } else {
        throw new ConnectException("GCP credentials not specified");
      }

      SecurityCenterSettings securityCenterSettings = SecurityCenterSettings.newBuilder()
          .setCredentialsProvider(FixedCredentialsProvider.create(ServiceAccountJwtAccessCredentials.fromStream(credsIS)))
          .build();
      this.sccClient = SecurityCenterClient.create(securityCenterSettings);
    } catch (ConnectException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectException("Failed to create SecurityCenterClient", e);
    }

  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {

    List<SourceRecord> allResult = new ArrayList();
    boolean done = false;
    while (!done) {
      String nextPageToken;
      ListFindingsPagedResponse fResp;
      Timestamp maxPollTs;
      try {
        maxPollTs = Timestamps.parse((String) offset.get(OFFSET_EVENT_TIME_KEY));
      } catch (ParseException e1) {
        throw new ConnectException("Offset value can not be parsed as RFC 3339 date:" + offset.get(OFFSET_EVENT_TIME_KEY));
      }
      List<SourceRecord> reqResult;

      try {

        ListFindingsRequest.Builder fReqB = ListFindingsRequest.newBuilder()
            .setParent((String) partition.get(PARTITION_PARENT_KEY))
            .setFilter(this.filter.replaceAll("\\{offset\\}", (String) offset.get(OFFSET_EVENT_TIME_KEY)))
            .setOrderBy(this.orderBy)
            .setPageSize(Math.min(this.pageSize, itemsToPoll));

        nextPageToken = (String) offset.get(OFFSET_NEXT_PAGE_TOKEN_KEY);

        if (StringUtils.isNotBlank(nextPageToken)) {
          fReqB.setPageToken(nextPageToken);
        }
        if (this.compareDuration != null) {
          fReqB.setCompareDuration(this.compareDuration);
        }

        fResp = sccClient.listFindings(fReqB.build());

        if (fResp == null || fResp.getPage() == null) {
          log.debug("null response for parent={}, offset={}", partition.get(PARTITION_PARENT_KEY), offset);
          return Collections.emptyList();
        }

        ImmutableList<ListFindingsResult> items = ImmutableList.copyOf(fResp.getPage().getValues());
        if (items.size() == 0) {
          done = true;
        }
        reqResult = new ArrayList<>(items.size());

        if (offset.containsKey(OFFSET_MAX_PAGE_TIME_KEY)) {
          maxPollTs = Timestamps.parse((String) offset.get(OFFSET_MAX_PAGE_TIME_KEY));
        }

        for (ListFindingsResult f : items) {
          Timestamp fTime = f.getFinding().getEventTime();
          if (Timestamps.compare(fTime, maxPollTs) > 0) {
            maxPollTs = fTime;
          }
          SourceRecord sr = new SourceRecord(partition, offset, topic, null, null, null, JSON_PRINTER.print(f));
          sr.headers().addString("gcscc.organizationId", this.orgId);
          sr.headers().addString("gcscc.source", partition.get(PARTITION_PARENT_KEY).toString());
          reqResult.add(sr);
        }
      } catch (Exception e) {
        throw new APIClientException("Poll failed", e);
      }

      nextPageToken = fResp.getNextPageToken();
      if (StringUtils.isNotBlank(nextPageToken)) {
        offset.put(OFFSET_NEXT_PAGE_TOKEN_KEY, nextPageToken);
        offset.put(OFFSET_MAX_PAGE_TIME_KEY, Timestamps.toString(maxPollTs));
      } else {
        offset.remove(OFFSET_NEXT_PAGE_TOKEN_KEY);
        offset.remove(OFFSET_MAX_PAGE_TIME_KEY);
        offset.put(OFFSET_EVENT_TIME_KEY, Timestamps.toString(maxPollTs));
        done = true;
      }
      allResult.addAll(reqResult);
      if (allResult.size() >= itemsToPoll) {
        done = true;
      }
    }

    return allResult;
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {

    List<String> parents = new ArrayList<>();

    if (this.sourceIds.isEmpty()) {
      log.debug("Retrieve sourceIds for organizationId = {}", this.orgId);

      OrganizationName organizationName = OrganizationName.of(this.orgId);
      ListSourcesRequest.Builder srequest = ListSourcesRequest.newBuilder().setParent(organizationName.toString());
      // Call the API.
      ListSourcesPagedResponse sresponse = sccClient.listSources(srequest.build());
      for (Source src : sresponse.iterateAll()) {
        log.debug("found source: {}.", src.toString());
        parents.add(src.getName());
      }
    } else {
      for (String sourceId : this.sourceIds) {
        parents.add(SourceName.of(this.orgId, sourceId).toString());
      }
    }

    log.debug("Configured {} sources for {}", parents.size(), this.orgId);
    List<Map<String, Object>> partitions = new ArrayList<>(parents.size());
    for (String parent : parents) {
      HashMap<String, Object> p = new HashMap<>(1);
      p.put(PARTITION_PARENT_KEY, parent);
      partitions.add(Collections.unmodifiableMap(p));
    }
    log.debug("Created {} partitions", partitions.size());

    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    Map<String, Object> offset = new HashMap<>(2);
    Timestamp initialTimestamp;
    try {
      initialTimestamp = Timestamps.parse(initialOffset);
    } catch (ParseException e) {
      throw new APIClientException("Initial offset parsing failed.", e);
    }
    offset.put(OFFSET_EVENT_TIME_KEY, Timestamps.toString(initialTimestamp));
    log.debug("For partition: {} Initial Offset configured: {}", partition, offset);
    return offset;
  }

  @Override
  public void close() {
    this.sccClient.close();
    this.sccClient = null;

  }

}
