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
package com.mckesson.kafka.connect.gsuite;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.admin.reports.Reports;
import com.google.api.services.admin.reports.ReportsScopes;
import com.google.api.services.admin.reports.model.Activities;
import com.google.api.services.admin.reports.model.Activity;
import com.mckesson.kafka.connect.source.APIClientException;
import com.mckesson.kafka.connect.source.PollableAPIClient;
import com.mckesson.kafka.connect.utils.SslUtils;

/**
 * https://developers.google.com/admin-sdk/reports/v1/reference/activities/list
 */
public class GSuiteAdminReportsAPIClient implements PollableAPIClient {

  private static final Logger log = LoggerFactory.getLogger(GSuiteAdminReportsAPIClient.class);
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  public static final String GCP_CREDENTIALS_FILE_PATH_CONFIG = "gcp.credentials.file.path";
  public static final String GCP_CREDENTIALS_JSON_CONFIG = "gcp.credentials.json";

  public static final String GSUITE_APP_NAMES_CONFIG = "gsuite.app.names";
  public static final String GSUITE_USER_KEYS_CONFIG = "gsuite.user.keys";
  public static final List<String> GSUITE_USER_KEYS_DEFAULT = Arrays.asList("all");
  public static final String INITIAL_OFFSET_DEFAULT = new DateTime(ZonedDateTime.now(ZoneId.of("UTC")).minusDays(180).toInstant().toEpochMilli()).toStringRfc3339();

  public static final String GSUITE_SA_USER_CONFIG = "gsuite.sa.user";
  public static final String GSUITE_SA_SCOPES_CONFIG = "gsuite.sa.scopes";
  public static final List<String> GSUITE_SA_SCOPES_DEFAULT = Arrays.asList(ReportsScopes.ADMIN_REPORTS_AUDIT_READONLY);

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(GSUITE_APP_NAMES_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "URI of Jira server. Required.")
      .define(GSUITE_USER_KEYS_CONFIG, ConfigDef.Type.LIST, GSUITE_USER_KEYS_DEFAULT, ConfigDef.Importance.HIGH, "List of userKeys. Default: all")
      .define(GCP_CREDENTIALS_FILE_PATH_CONFIG, Type.STRING, null, Importance.HIGH, "The path to the GCP credentials file")
      .define(GCP_CREDENTIALS_JSON_CONFIG, Type.STRING, null, Importance.HIGH, "GCP JSON credentials")
      .define(INITIAL_OFFSET_CONFIG, Type.STRING, INITIAL_OFFSET_DEFAULT, Importance.HIGH, "Initial StartTime")

      .define(GSUITE_SA_SCOPES_CONFIG, ConfigDef.Type.LIST, GSUITE_SA_SCOPES_DEFAULT, ConfigDef.Importance.HIGH, "SA scopes")
      .define(GSUITE_SA_USER_CONFIG, Type.STRING, null, Importance.HIGH, "SA user")

      .withClientSslSupport();

  private static final String PARTITION_USER_KEY = "_P_USER";
  private static final String PARTITION_APP_KEY = "_P_APP";
  private static final String PARTITION_PROJECT_ID_KEY = "_P_PROJECT_ID";

  private static final String OFFSET_START_DATE_KEY = "_START_DATE";
  private static final String OFFSET_NEXT_PAGE_TOKEN_KEY = "_NEXT_PAGE_TOKEN";
  private static final String OFFSET_MAX_PAGE_DT_KEY = "_MAX_PAGE_DT";

  private String projectId;
  private List<String> activityAppNames;
  private List<String> activityUserKeys;
  private String initialOffset;

  private Reports reportService;

  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);

    NetHttpTransport httpTransport;
    try {
      httpTransport = new NetHttpTransport.Builder().setSslSocketFactory(SslUtils.createSSLContext(new SimpleConfig(CONFIG_DEF, configs)).getSocketFactory()).build();
    } catch (Exception e) {
      throw new ConnectException("Failed to create http transport", e);
    }

    GoogleCredential credential;
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

      String saUser = conf.getString(GSUITE_SA_USER_CONFIG);
      List<String> saScopes = conf.getList(GSUITE_SA_SCOPES_CONFIG);
      credential = createScoped(GoogleCredential.fromStream(credsIS), saUser, saScopes);
      this.projectId = credential.getServiceAccountProjectId();
    } catch (ConnectException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectException("Failed to create GCP credentials", e);
    }

    this.activityAppNames = conf.getList(GSUITE_APP_NAMES_CONFIG);
    this.activityUserKeys = conf.getList(GSUITE_USER_KEYS_CONFIG);
    this.initialOffset = conf.getString(INITIAL_OFFSET_CONFIG);

    reportService = new Reports.Builder(httpTransport, JSON_FACTORY, credential).build();

  }

  public GoogleCredential createScoped(GoogleCredential credential, String user, Collection<String> scopes) {
    if (credential.getServiceAccountPrivateKey() == null) {
      return credential;
    }

    return new GoogleCredential.Builder()
        .setServiceAccountPrivateKey(credential.getServiceAccountPrivateKey())
        .setServiceAccountPrivateKeyId(credential.getServiceAccountPrivateKeyId())
        .setServiceAccountId(credential.getServiceAccountId())
        .setServiceAccountProjectId(credential.getServiceAccountProjectId())
        .setServiceAccountUser(StringUtils.isBlank(user) ? credential.getServiceAccountUser() : user)
        .setServiceAccountScopes(scopes)
        .setTokenServerEncodedUrl(credential.getTokenServerEncodedUrl())
        .setTransport(credential.getTransport())
        .setJsonFactory(credential.getJsonFactory())
        .setClock(credential.getClock())
        .build();
  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {

    String nextPageToken;
    Activities acts;
    List<SourceRecord> records;
    DateTime maxDt = new DateTime(0);
    try {
      Reports.Activities.List listService = reportService.activities().list((String) partition.get(PARTITION_USER_KEY), (String) partition.get(PARTITION_APP_KEY));

      listService.setStartTime((String) offset.get(OFFSET_START_DATE_KEY));
      listService.setMaxResults(Math.min(itemsToPoll, 1000));

      nextPageToken = (String) offset.get(OFFSET_NEXT_PAGE_TOKEN_KEY);
      if (StringUtils.isNotBlank(nextPageToken)) {
        listService.setPageToken(nextPageToken);
      }

      acts = listService.setPrettyPrint(false).execute();

      List<Activity> items = acts.getItems();

      if (items == null || items.size() == 0) {
        log.debug("empty response for applicationName={}, userKey={}, offset={}", partition.get(PARTITION_APP_KEY), partition.get(PARTITION_USER_KEY), offset);
        return Collections.emptyList();
      }

      records = new ArrayList<>(items.size());
      if (offset.containsKey(OFFSET_MAX_PAGE_DT_KEY)) {
        maxDt = DateTime.parseRfc3339((String) offset.get(OFFSET_MAX_PAGE_DT_KEY));
      }

      for (Activity a : items) {
        DateTime aDt = a.getId().getTime();
        if (aDt != null && aDt.getValue() > maxDt.getValue()) {
          maxDt = aDt;
        }
        SourceRecord sr = new SourceRecord(partition, offset, topic, null, null, null, a.toString());
        sr.headers().addString("gsuite.appName", partition.get(PARTITION_APP_KEY).toString());
        sr.headers().addString("gsuite.projectId", partition.get(PARTITION_PROJECT_ID_KEY).toString());
        records.add(sr);
      }
    } catch (Exception e) {
      throw new APIClientException("Poll failed", e);
    }

    nextPageToken = acts.getNextPageToken();
    if (StringUtils.isNotBlank(nextPageToken)) {
      offset.put(OFFSET_NEXT_PAGE_TOKEN_KEY, nextPageToken);
      offset.put(OFFSET_MAX_PAGE_DT_KEY, maxDt.toStringRfc3339());
    } else {
      offset.remove(OFFSET_NEXT_PAGE_TOKEN_KEY);
      offset.remove(OFFSET_MAX_PAGE_DT_KEY);
      offset.put(OFFSET_START_DATE_KEY, maxDt.toStringRfc3339());
    }

    return records;
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {
    List<Map<String, Object>> partitions = new ArrayList<>(activityUserKeys.size() * activityAppNames.size());
    for (String userKey : activityUserKeys) {
      for (String appName : activityAppNames) {
        HashMap<String, Object> p = new HashMap<>(3);
        p.put(PARTITION_PROJECT_ID_KEY, this.projectId);
        p.put(PARTITION_USER_KEY, userKey);
        p.put(PARTITION_APP_KEY, appName);
        partitions.add(Collections.unmodifiableMap(p));
      }
    }
    log.debug("Created {} partitions for projectId: {},  userKeys: {}, applications: {} ==> {}", partitions.size(), projectId, activityUserKeys, activityAppNames, partitions);

    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    Map<String, Object> offset = new HashMap<>(2);
    DateTime startDate = DateTime.parseRfc3339(initialOffset); //validate initial.offset string 
    offset.put(OFFSET_START_DATE_KEY, startDate.toStringRfc3339());
    log.debug("For partition: {} Initial Offset configured: {}", partition, offset);
    return offset;
  }

  @Override
  public void close() {
    reportService = null;

  }

}
