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

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mckesson.kafka.connect.source.APIClientException;
import com.mckesson.kafka.connect.source.PollableAPIClient;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.EventPosition;
import com.microsoft.azure.eventhubs.PartitionReceiver;

public class AzureEventHubClient implements PollableAPIClient {

  private static final Logger log = LoggerFactory.getLogger(AzureEventHubClient.class);
  private final ObjectMapper mapper = new ObjectMapper();

  /* CONFIGS */
  public static final String ENDPOINT_CONFIG = "azure.eventhub.endpoint";
  public static final String NAME_CONFIG = "azure.eventhub.name";
  public static final String PARTITIONS_CONFIG = "azure.eventhub.partitions";
  public static final String KEYNAME_CONFIG = "azure.eventhub.keyname";
  public static final String KEY_CONFIG = "azure.eventhub.key";
  public static final String RECEIVER_TIMEOUT_CONFIG = "azure.eventhub.receiver.timeout";
  public static final Long RECEIVER_TIMEOUT_DEFAULT = 2000L;
  public static final String RECEIVER_INIT_OFFSET_DEFAULT = "@latest";
  public static final String RECEIVER_PREFETCH_COUNT_CONFIG = "azure.eventhub.receiver.prefetchCount";
  public static final Integer RECEIVER_PREFETCH_COUNT_DEFAULT = 300;
  public static final String RECEIVER_CACHING_ENABLED_CONFIG = "azure.eventhub.receiver.caching.enabled";
  public static final Boolean RECEIVER_CACHING_ENABLED_DEFAULT = false;
  public static final String RECEIVER_EXTRACT_RECORDS_CONFIG = "azure.eventhub.receiver.extract.records.enabled";
  public static final Boolean RECEIVER_EXTRACT_RECORDS_DEFAULT = Boolean.TRUE;
  public static final String RECEIVER_EXTRACT_RECORDS_PTR_CONFIG = "azure.eventhub.receiver.extract.records.pointer";
  public static final String RECEIVER_EXTRACT_RECORDS_PTR_DEFAULT = "/records";

  public static final String CONSUMER_GROUP_CONFIG = "azure.eventhub.consumer.group";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(ENDPOINT_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "URI of endpoint which can be used to connect to the EventHub instance")
      .define(NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Name of the Event Hub to connect to")
      .define(PARTITIONS_CONFIG, ConfigDef.Type.LIST, Collections.EMPTY_LIST, ConfigDef.Importance.HIGH, "Partitions to read. Default read all available")
      .define(KEYNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "SAS key name.")
      .define(KEY_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "SAS key")
      .define(RECEIVER_TIMEOUT_CONFIG, ConfigDef.Type.LONG, RECEIVER_TIMEOUT_DEFAULT, ConfigDef.Importance.MEDIUM, "Receive timeout")
      .define(RECEIVER_CACHING_ENABLED_CONFIG, ConfigDef.Type.BOOLEAN, RECEIVER_CACHING_ENABLED_DEFAULT, ConfigDef.Importance.MEDIUM, "Receivers cache enabled: default=true")
      .define(RECEIVER_PREFETCH_COUNT_CONFIG, ConfigDef.Type.INT, RECEIVER_PREFETCH_COUNT_DEFAULT, ConfigDef.Importance.LOW, "number of events that can be pre-fetched and cached at the PartitionReceiver. default: 300")
      .define(PollableAPIClient.INITIAL_OFFSET_CONFIG, ConfigDef.Type.STRING, RECEIVER_INIT_OFFSET_DEFAULT, ConfigDef.Importance.LOW, "Initial offset. default: '@latest'")
      .define(CONSUMER_GROUP_CONFIG, ConfigDef.Type.STRING, EventHubClient.DEFAULT_CONSUMER_GROUP_NAME, ConfigDef.Importance.LOW, "Consumer group")
      .define(RECEIVER_EXTRACT_RECORDS_CONFIG, ConfigDef.Type.BOOLEAN, RECEIVER_EXTRACT_RECORDS_DEFAULT, ConfigDef.Importance.LOW, "Extract records from json. Default: true")
      .define(RECEIVER_EXTRACT_RECORDS_PTR_CONFIG, ConfigDef.Type.STRING, RECEIVER_EXTRACT_RECORDS_PTR_DEFAULT, ConfigDef.Importance.LOW,
          "Pointer expression. See \"https://tools.ietf.org/html/draft-ietf-appsawg-json-pointer-03\". Default: '/records'");

  private EventHubClient ehClient;

  private ExecutorService executorService = Executors.newSingleThreadExecutor();

  private Boolean cacheRecievers;
  private Map<String, PartitionReceiver> receiversCache;

  private String endpoint;
  private String hubName;
  private String consumerGroup;
  private String initOffset;
  private Long receiveTimeout;
  private Integer prefetchCount;
  private List<String> configuredPartitions;

  private Boolean extractRecords;
  private JsonPointer extractPtr;

  public static final String PARTITION_ID_KEY = "__PARTITION_ID";
  public static final String PARTITION_CONSUMER_GROUP_KEY = "__CONSUMER_GROUP";
  public static final String PARTITION_ENDPOINT_KEY = "__ENDPOINT";
  public static final String PARTITION_HUBNAME_KEY = "__HUBNAME";

  public static final String OFFSET_KEY_OFFSET = "OFFSET";

  @Override
  public void configure(Map<String, ?> configs) {

    String keyName;
    Password key;
    try {
      final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
      this.receiveTimeout = config.getLong(RECEIVER_TIMEOUT_CONFIG);
      this.prefetchCount = config.getInt(RECEIVER_PREFETCH_COUNT_CONFIG);
      this.consumerGroup = config.getString(CONSUMER_GROUP_CONFIG);
      this.initOffset = config.getString(PollableAPIClient.INITIAL_OFFSET_CONFIG);
      this.endpoint = config.getString(ENDPOINT_CONFIG);
      this.hubName = config.getString(NAME_CONFIG);
      this.cacheRecievers = config.getBoolean(RECEIVER_CACHING_ENABLED_CONFIG);
      this.configuredPartitions = config.getList(PARTITIONS_CONFIG);
      keyName = config.getString(KEYNAME_CONFIG);
      key = config.getPassword(KEY_CONFIG);

      this.extractRecords = config.getBoolean(RECEIVER_EXTRACT_RECORDS_CONFIG);
      this.extractPtr = JsonPointer.compile(config.getString(RECEIVER_EXTRACT_RECORDS_PTR_CONFIG));

      if (cacheRecievers) {
        receiversCache = new HashMap<>();
      }

    } catch (Exception e) {
      throw new ConnectException("Failed to read config", e);
    }

    try {
      final ConnectionStringBuilder connStr = new ConnectionStringBuilder()
          .setEndpoint(new URI(this.endpoint))
          .setEventHubName(hubName)
          .setSasKeyName(keyName)
          .setSasKey(key.value());
      this.ehClient = EventHubClient.createSync(connStr.toString(), executorService);

    } catch (Exception e) {
      throw new ConnectException("Failed to create EventHub Client", e);
    }

  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {

    String partitionId = partition.get(PARTITION_ID_KEY).toString();
    String consumerGroup = partition.get(PARTITION_CONSUMER_GROUP_KEY).toString();
    String offsetValue = offset.get(OFFSET_KEY_OFFSET).toString();

    List<SourceRecord> result = new ArrayList<>();

    PartitionReceiver receiver = null;
    try {
      receiver = getReceiver(partitionId, consumerGroup, offsetValue);
      boolean doneForPartition = false;
      while (!doneForPartition) {
        List<SourceRecord> partResult = new ArrayList<>();

        CompletableFuture<Iterable<EventData>> ftr = receiver.receive(this.prefetchCount);
        //wait till done with respect to stop signal.
        long startTime = System.currentTimeMillis();
        while (!ftr.isDone()) {
          if (stop != null && stop.get()) {
            ftr.cancel(true);
            log.debug("Stop signal !!! Exit immediately");
            return Collections.emptyList();
          }
        }
        log.trace("receiver.receive time till done millis: {}", System.currentTimeMillis() - startTime);
        if (ftr.isCompletedExceptionally()) {
          log.debug("recieve finished Exceptionally");
        }
        String newOffset = offset.get(OFFSET_KEY_OFFSET).toString();
        Iterable<EventData> receivedEvents = ftr.get();
        if (receivedEvents != null) {
          for (EventData evt : receivedEvents) {

            try {
              if (extractRecords) {
                JsonNode node = mapper.readValue(evt.getBytes(), JsonNode.class);
                JsonNode valueNode = node.at(extractPtr);
                if (valueNode.isMissingNode()) {
                  partResult.add(createSourceRecord(partition, offset, topic, mapper.writeValueAsString(node)));
                } else if (valueNode.isArray()) {
                  for (JsonNode rec : valueNode) {
                    partResult.add(createSourceRecord(partition, offset, topic, mapper.writeValueAsString(rec)));
                  }
                } else {
                  partResult.add(createSourceRecord(partition, offset, topic, mapper.writeValueAsString(valueNode)));
                }
              } else {
                partResult.add(createSourceRecord(partition, offset, topic, new String(evt.getBytes())));
              }
              String recOffset = evt.getSystemProperties().getOffset();
              if (!recOffset.equals(newOffset)) {
                newOffset = recOffset;
              }
            } catch (Exception e) {
              String rawData = evt == null ? "!_EMPTY_!" : new String(evt.getBytes());
              log.error("Failed to read EventData from raw: {}\n", rawData, e);
              log.error("Error: ", e);
              Map<String, String> errorData = new HashMap<>();
              errorData.put("rawData", rawData);
              errorData.put("errorMessage", e.getMessage());
              errorData.put("javaStackTrace", ExceptionUtils.getStackTrace(e));
              errorData.put("messageType", "KAFKA_ERROR");
              errorData.put("offset", offset.get(OFFSET_KEY_OFFSET).toString());
              SourceRecord r = new SourceRecord(partition, offset, topic, null, null, null, mapper.writeValueAsString(errorData));
              r.headers().addString("error", e.getMessage());
              r.headers().addBoolean("failed", true);
              partResult.add(r);
              log.warn("Failed record added to topic:{} from partition:{}, offset:{}", topic, partition, offset);
            }
          }
        }
        offset.put(OFFSET_KEY_OFFSET, newOffset);
        result.addAll(partResult);
        doneForPartition = partResult.size() == 0 || result.size() >= itemsToPoll;
      }
      log.debug("Poll successfully complete for topic:{}, partition:{}, poll total size so far: {}", topic, partitionId, result.size());

    } catch (Exception e) {
      log.debug("Poll failed for topic:{}, partition:{}", topic, partition, e);
      throw new AzureClientException(e);
    } finally {
      closeReceiver(consumerGroup, receiver);

    }
    return result;

  }

  private SourceRecord createSourceRecord(Map<String, Object> partition, Map<String, Object> offset, String topic, String data) {
    SourceRecord sr = new SourceRecord(partition, offset, topic, null, null, Schema.OPTIONAL_STRING_SCHEMA, data);
    Object hubName = partition.get(PARTITION_HUBNAME_KEY);
    if (hubName != null) {
      sr.headers().addString("azure.eventhub.name", hubName.toString());
    }
    return sr;
  }

  private void closeReceiver(String cGroup, PartitionReceiver receiver) {
    //NO-OP if caching enabled;
    if (cacheRecievers || receiver == null) {
      return;
    }

    String partitionId = receiver.getPartitionId();
    String receiverId = String.format("%s/ConsumerGroups/%s/Partitions/%s", this.hubName, cGroup, partitionId);
    log.trace("closed receiver for {}", receiverId);
    receiver.close();
    /*
    try {
      
    } catch (EventHubException e) {
      log.error("Failed to close receiver: " + receiverId, e);
    } 
    */

  }

  private PartitionReceiver getReceiver(String partitionId, String cGroup, String offsetValue) throws AzureClientException {

    PartitionReceiver receiver = null;
    String receiverId = String.format("%s/ConsumerGroups/%s/Partitions/%s", this.hubName, cGroup, partitionId);
    if (this.cacheRecievers && receiversCache.containsKey(receiverId)) {
      receiver = receiversCache.get(receiverId);
    }
    if (receiver == null) {
      try {
        receiver = ehClient.createReceiverSync(consumerGroup, partitionId, EventPosition.fromOffset(offsetValue));
        receiver.setPrefetchCount(this.prefetchCount);
        receiver.setReceiveTimeout(Duration.ofMillis(this.receiveTimeout));
        if (this.cacheRecievers) {
          receiversCache.put(receiverId, receiver);
        }
      } catch (EventHubException e1) {
        throw new AzureClientException("failed to create receiver", e1);
      }
    }
    return receiver;

  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {
    List<Map<String, Object>> partitions;
    try {
      List<String> partitionIDs;
      if (configuredPartitions != null && configuredPartitions.size() > 0) {
        log.debug("Using configured list of partitions: {}", configuredPartitions);
        partitionIDs = new ArrayList<>(configuredPartitions);
      } else {
        log.debug("Obtain all available partitions...");
        partitionIDs = Arrays.asList(ehClient.getRuntimeInformation().get().getPartitionIds());
      }
      partitions = new ArrayList<>(partitionIDs.size());
      for (String pID : partitionIDs) {
        Map<String, String> p = new HashMap<>(4);
        p.put(PARTITION_ENDPOINT_KEY, this.endpoint);
        p.put(PARTITION_HUBNAME_KEY, this.hubName);
        p.put(PARTITION_ID_KEY, pID);
        p.put(PARTITION_CONSUMER_GROUP_KEY, consumerGroup);
        partitions.add(Collections.unmodifiableMap(p));
      }

    } catch (Exception e) {
      throw new APIClientException("Failed to read partition IDs", e);
    }
    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    Map<String, Object> offset = new HashMap<>(1);
    offset.put(OFFSET_KEY_OFFSET, initOffset);
    return offset;
  }

  @Override
  public void close() {

    if (receiversCache != null && receiversCache.size() > 0) {
      log.info("Closing all PartitionReceivers on client close");
      for (Map.Entry<String, PartitionReceiver> r : receiversCache.entrySet()) {

        r.getValue().close();
        /*
        try {
          
        } catch (EventHubException e) {
          log.warn("Failed to close PartitionReceiver for:" + r.getKey(), e);
        }
        */

      }
      receiversCache.clear();
      log.info("PartitionReceivers are closed.");
    }
    ehClient.close();
    executorService.shutdown();
    log.info("AzureEventHubClient closed.");
  }

}
