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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.force.api.DescribeSObject;
import com.force.api.DescribeSObject.Field;
import com.mckesson.kafka.connect.source.APIClientException;

public class SobjectPollableAPIClient extends SalesforceAPIClient {
  private static final Logger log = LoggerFactory.getLogger(EventLogPollableAPIClient.class);

  public static final String SF_SOBJECTS_CONFIG = "sfdc.sobjects";
  public static final String SF_QUERY_LIMIT_CONFIG = "sfdc.queryLimit";
  public static final int SF_QUERY_LIMIT_DEFAULT = 1000;
  private static final ConfigDef CLIENT_CONFIG_DEF = new ConfigDef()
      .define(SF_SOBJECTS_CONFIG, Type.LIST, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "SObjects to read")
      .define(SF_QUERY_LIMIT_CONFIG, Type.INT, SF_QUERY_LIMIT_DEFAULT, Importance.LOW, "Query limit");

  public static final String SOBJECT_NAME_CONFIG = "soName";
  public static final String SOBJECT_QUERY_CONFIG = "soql";
  public static final String SOBJECT_FIELDS_CONFIG = "fields";
  public static final String SOBJECT_ORDER_BY_CONFIG = "orderBy";

  private static final ConfigDef SOBJECT_CONFIG_DEF = new ConfigDef()
      .define(SOBJECT_NAME_CONFIG, Type.STRING, null, Importance.MEDIUM, "SObjectname")
      .define(SOBJECT_QUERY_CONFIG, Type.STRING, null, Importance.MEDIUM, "SObject soql")
      .define(SOBJECT_FIELDS_CONFIG, Type.LIST, null, Importance.MEDIUM, "SObject fields")
      .define(SOBJECT_ORDER_BY_CONFIG, Type.STRING, null, Importance.MEDIUM, "SObject order/trck by field");

  private static final String PARTITION_NAME_KEY = "_sObjectName";
  private static final String PARTITION_QUERY_KEY = "_sObjectQuery";
  private static final String PARTITION_ORDER_BY_KEY = "_sObjectOrderBy";
  private static final String OFFSET_LOG_TIME_KEY = "__LastModifiedDate";

  private static final String HEADER_SOBJECT_NAME = "sfdc.sobject";

  private SimpleConfig clientConf;
  private List<String> sobjects;
  private Integer queryLimit;

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    this.clientConf = new SimpleConfig(CLIENT_CONFIG_DEF, configs);
    this.sobjects = clientConf.getList(SF_SOBJECTS_CONFIG);
    this.queryLimit = clientConf.getInt(SF_QUERY_LIMIT_CONFIG);
  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {
    log.debug("Executiong poll for sObject: {}, with offset: {}", partition.get(PARTITION_NAME_KEY), offset);
    String soql = partition.get(PARTITION_QUERY_KEY)
        .toString()
        .replaceAll("\\{offset\\}", offset.get(OFFSET_LOG_TIME_KEY).toString())
        .replaceAll("\\{limit\\}", "" + Math.min(itemsToPoll, queryLimit));

    List<Map> qr = executeQuery(soql, offset, Map.class);

    if (qr.size() == 0) {
      log.debug("No new records for:{}", partition.get(PARTITION_NAME_KEY));
      return Collections.emptyList();
    }

    List<SourceRecord> pollResult = new ArrayList<>(qr.size());
    for (Map m : qr) {
      try {
        SourceRecord rec = new SourceRecord(partition, offset, topic, null, null, null, jacksonObjectMapper.writeValueAsString(m));
        rec.headers().addString(HEADER_SOBJECT_NAME, partition.get(PARTITION_NAME_KEY).toString());
        pollResult.add(rec);
      } catch (JsonProcessingException ex) {
        SourceRecord failedRec = new SourceRecord(partition, offset, topic, null, null, null, m.toString());
        failedRec.headers().addBoolean("failed", true);
        failedRec.headers().addString("errorMsg", ex.getMessage());
        pollResult.add(failedRec);
      }
    }
    String newoffset = qr.get(qr.size() - 1).get(partition.get(PARTITION_ORDER_BY_KEY)).toString();
    log.debug("Offset for {} updated to: {}", partition.get(PARTITION_NAME_KEY), newoffset);
    offset.put(OFFSET_LOG_TIME_KEY, newoffset);
    return pollResult;
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {
    log.debug("Create partitions for:{}", sobjects);
    List<Map<String, Object>> partitions = new ArrayList<>();
    for (String soAlias : sobjects) {
      SimpleConfig soConf = new SimpleConfig(SOBJECT_CONFIG_DEF, clientConf.originalsWithPrefix(SF_SOBJECTS_CONFIG + "." + soAlias + "."));
      String soName = StringUtils.isNotBlank(soConf.getString(SOBJECT_NAME_CONFIG)) ? soConf.getString(SOBJECT_NAME_CONFIG) : soAlias;
      List<String> soFields = soConf.getList(SOBJECT_FIELDS_CONFIG);
      String soOrderBy = soConf.getString(SOBJECT_ORDER_BY_CONFIG);
      String soQuery = soConf.getString(SOBJECT_QUERY_CONFIG);

      Map<String, Object> p = buildPartition(soName, soFields, soOrderBy, soQuery);
      log.debug("Created partition for alias: {} => {}", soAlias, p);
      partitions.add(p);

    }

    return partitions;
  }

  private Map<String, Object> buildPartition(String soName, List<String> soFields, String soOrderBy, String soQuery) throws APIClientException {

    if (StringUtils.isBlank(soName)) {
      throw new IllegalArgumentException("sObject name can not be blank");
    }
    DescribeSObject descr = null;
    if (soFields == null || soFields.size() == 0 || StringUtils.isBlank(soOrderBy)) {
      descr = forceApi.describeSObject(soName);
    }
    String selectedOrderBy = soOrderBy;
    if (StringUtils.isBlank(selectedOrderBy)) {
      //either LastModifiedDate or 'last of datetime type' will be selected
      for (Field f : descr.getFields()) {
        if ("LastModifiedDate".equals(f.getName())) {
          selectedOrderBy = "LastModifiedDate";
          break; //LastModifiedDate is always used if available
        }
        if ("datetime".equals(f.getType())) {
          selectedOrderBy = f.getName();
        }
      }
    }
    if (StringUtils.isBlank(selectedOrderBy)) {
      throw new APIClientException("Failed to find 'orderBy' for " + soName);
    }

    //use or find fields from descr
    LinkedHashSet<String> selectedFields;
    if (soFields != null && soFields.size() > 0) {
      selectedFields = new LinkedHashSet<>(soFields);
    } else {
      selectedFields = new LinkedHashSet<>(descr.getFields().size());
      descr.getFields().forEach(e -> {
        selectedFields.add(e.getName());
      });
    }

    //build query
    String selectedQuery = soQuery;
    if (StringUtils.isBlank(selectedQuery)) {
      selectedQuery = new StringBuilder()
          .append("select ").append(StringUtils.join(selectedFields, ", "))
          .append("\n from ").append(soName)
          .append("\n where ").append(selectedOrderBy).append(" > {offset}")
          .append("\n order by ").append(selectedOrderBy)
          .append("\n limit {limit}")
          .toString();
    }

    Map<String, String> partition = new HashMap<>(3);
    partition.put(PARTITION_NAME_KEY, soName);
    partition.put(PARTITION_ORDER_BY_KEY, selectedOrderBy);
    partition.put(PARTITION_QUERY_KEY, selectedQuery.toString());
    return Collections.unmodifiableMap(partition);

  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    Map<String, Object> offset = new HashMap<>(1);
    offset.put(OFFSET_LOG_TIME_KEY, this.initialOffset);
    return offset;
  }

}
