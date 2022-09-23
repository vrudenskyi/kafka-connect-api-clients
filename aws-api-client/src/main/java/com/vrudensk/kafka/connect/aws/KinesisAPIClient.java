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
package com.vrudensk.kafka.connect.aws;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.ProvisionedThroughputExceededException;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;
import com.vrudenskyi.kafka.connect.source.APIClientException;
import com.vrudenskyi.kafka.connect.source.PollableAPIClient;

public class KinesisAPIClient implements PollableAPIClient {

  private static final Logger log = LoggerFactory.getLogger(KinesisAPIClient.class);

  public static final String AWS_ACCESS_KEY_ID_CONF = "aws.access.key.id";
  public static final String AWS_SECRET_KEY_ID_CONF = "aws.secret.key.id";
  public static final String TOPIC_CONF = "kafka.topic";

  public static final String KINESIS_STREAM_NAME_CONF = "kinesis.stream";
  public static final String KINESIS_REGION_CONF = "kinesis.region";
  public static final String KINESIS_RECORD_LIMIT_CONF = "kinesis.record.limit";
  public static final String KINESIS_RECORD_BUILDER_CONF = "kinesis.record.builder";

  static final String STREAM_NAME_DOC = "The Kinesis stream to read from.";
  static final String AWS_ACCESS_KEY_ID_DOC = "aws.access.key.id";
  static final String AWS_SECRET_KEY_ID_DOC = "aws.secret.key.id";
  static final String KINESIS_REGION_DOC = "The AWS region for the Kinesis stream.";
  static final String KINESIS_RECORD_LIMIT_DOC = "The number of records to read in each poll of the Kinesis shard.";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(AWS_ACCESS_KEY_ID_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, AWS_ACCESS_KEY_ID_DOC)
      .define(AWS_SECRET_KEY_ID_CONF, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH, AWS_SECRET_KEY_ID_DOC)
      .define(KINESIS_STREAM_NAME_CONF, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, STREAM_NAME_DOC)
      .define(KINESIS_REGION_CONF, ConfigDef.Type.STRING, Regions.US_EAST_1.toString(), ConfigDef.Importance.MEDIUM, KINESIS_REGION_DOC)
      .define(KINESIS_RECORD_LIMIT_CONF, ConfigDef.Type.INT, 500, ConfigDef.Range.between(1, 10000), ConfigDef.Importance.MEDIUM, KINESIS_RECORD_LIMIT_DOC)
      .define(PollableAPIClient.INITIAL_OFFSET_CONFIG, ConfigDef.Type.STRING, ShardIteratorType.TRIM_HORIZON.toString(), ConfigDef.Importance.LOW, "Initial offset")
      .define(KINESIS_RECORD_BUILDER_CONF, ConfigDef.Type.CLASS, KinesisRecordBuilder.class, ConfigDef.Importance.LOW, "Implementation  Kinesis Record -> SourceRecord builder");

  private String awsAccessKeyId;
  private Password awsSecretKeyId;
  private String kinesisStreamName;
  private int kinesisRecordLimit;
  private Regions kinesisRegion;
  private String initialOffset;

  AmazonKinesis kinesisClient;
  Map<Map<String, Object>, GetRecordsRequest> requests = new HashMap<>();
  private SourceRecordBuilder<Record> recordBuilder;

  public static final String PARTITION_REGION_NAME_KEY = "region";
  public static final String PARTITION_STREAM_NAME_KEY = "streamName";
  public static final String PARTITION_SHARD_ID_KEY = "shardId";

  public static final String OFFSET_ITER_TYPE_KEY = "iteratorType";
  public static final String OFFSET_SEQUENCE_NUMBER_KEY = "sequenceNumber";
  public static final String OFFSET_SHARD_ITERATOR_KEY = "shardIterator";

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);

    this.awsAccessKeyId = conf.getString(AWS_ACCESS_KEY_ID_CONF);
    this.awsSecretKeyId = conf.getPassword(AWS_SECRET_KEY_ID_CONF);
    this.kinesisStreamName = conf.getString(KINESIS_STREAM_NAME_CONF);
    this.kinesisRegion = Regions.valueOf(conf.getString(KINESIS_REGION_CONF));
    this.kinesisRecordLimit = conf.getInt(KINESIS_RECORD_LIMIT_CONF);
    this.initialOffset = conf.getString(INITIAL_OFFSET_CONFIG);

    this.recordBuilder = conf.getConfiguredInstance(KINESIS_RECORD_BUILDER_CONF, SourceRecordBuilder.class);
    if (this.recordBuilder instanceof Configurable) {
      ((Configurable) this.recordBuilder).configure(conf.originalsWithPrefix(KINESIS_RECORD_BUILDER_CONF + "."));
    }

    AWSCredentialsProvider creds = new AWSStaticCredentialsProvider(new BasicAWSCredentials(this.awsAccessKeyId, this.awsSecretKeyId.value()));

    this.kinesisClient = AmazonKinesisClient.builder()
        .withCredentials(creds)
        .withRegion(kinesisRegion)
        .build();
  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {

    List<SourceRecord> records = new ArrayList<>();

    try {
      GetRecordsRequest req = getRequest(partition, offset);
      req.setLimit(Math.min(kinesisRecordLimit, itemsToPoll));
      GetRecordsResult recordsResult = this.kinesisClient.getRecords(req);

      List<Record> resultRecords = recordsResult.getRecords();

      if (resultRecords != null && resultRecords.size() > 0) {
        String lastSeqNum = resultRecords.get(resultRecords.size() - 1).getSequenceNumber();
        offset.put(OFFSET_ITER_TYPE_KEY, ShardIteratorType.AFTER_SEQUENCE_NUMBER.name());
        offset.put(OFFSET_SEQUENCE_NUMBER_KEY, lastSeqNum);
      }

      log.trace("Received {} records  for partition: {}", resultRecords.size(), partition);
      for (Record record : resultRecords) {
        records.addAll(recordBuilder.buildRecords(topic, partition, offset, null, record));
      }
      log.trace("Produced {} SourceRecord(s) for partition: {}", records.size(), partition);
      req.setShardIterator(recordsResult.getNextShardIterator());
    } catch (ProvisionedThroughputExceededException ex) {
      throw new RetriableException(ex);
    }

    return records;
  }

  private GetRecordsRequest getRequest(Map<String, Object> partition, Map<String, Object> offset) {
    if (!requests.containsKey(partition)) {
      log.debug("getRequest partition: {}, offset: {}", partition, offset);

      GetShardIteratorRequest shardIteratorRequest = new GetShardIteratorRequest()
          .withStreamName(partition.get(PARTITION_STREAM_NAME_KEY).toString())
          .withShardId(partition.get(PARTITION_SHARD_ID_KEY).toString());

      if (offset.containsKey(OFFSET_SEQUENCE_NUMBER_KEY)) {
        String startingSequenceNumber = (String) offset.get(OFFSET_SEQUENCE_NUMBER_KEY);
        log.debug("\tStarting iterator after last processed sequence number of '{}'", startingSequenceNumber);
        shardIteratorRequest.setShardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
        shardIteratorRequest.setStartingSequenceNumber(startingSequenceNumber);
      } else {
        log.debug("\tSetting Shard Iterator Type to {} for {}", offset.get(OFFSET_ITER_TYPE_KEY), partition);
        shardIteratorRequest.setShardIteratorType(offset.get(OFFSET_ITER_TYPE_KEY).toString());
      }

      GetShardIteratorResult shardIteratorResult = this.kinesisClient.getShardIterator(shardIteratorRequest);
      log.debug("\tSet Shard Iterator {}", shardIteratorResult.getShardIterator());

      GetRecordsRequest recordsRequest = new GetRecordsRequest()
          .withLimit(this.kinesisRecordLimit)
          .withShardIterator(shardIteratorResult.getShardIterator());

      requests.put(partition, recordsRequest);
    }

    return requests.get(partition);
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {

    List<Map<String, Object>> partitions = new ArrayList<>();

    ListShardsRequest lsReq = new ListShardsRequest()
        .withStreamName(this.kinesisStreamName);
    while (lsReq != null) {
      ListShardsResult lsRes = this.kinesisClient.listShards(lsReq);
      List<Shard> shards = lsRes.getShards();

      for (Shard shard : shards) {
        HashMap<String, Object> partition = new HashMap<>(3);
        partition.put(PARTITION_REGION_NAME_KEY, this.kinesisRegion.name());
        partition.put(PARTITION_STREAM_NAME_KEY, this.kinesisStreamName);
        partition.put(PARTITION_SHARD_ID_KEY, shard.getShardId());
        partitions.add(partition);
      }
      if (StringUtils.isNotBlank(lsRes.getNextToken())) {
        lsReq = new ListShardsRequest()
            .withStreamName(this.kinesisStreamName)
            .withNextToken(lsRes.getNextToken());

      } else {
        lsReq = null;
      }
    }

    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {

    Map<String, Object> offset;
    if (initialOffset.contains(":")) {
      String[] parts = initialOffset.split(":");
      offset = new HashMap<>(2);
      offset.put(OFFSET_ITER_TYPE_KEY, ShardIteratorType.fromValue(parts[0]));
      offset.put(OFFSET_SEQUENCE_NUMBER_KEY, parts[1]);
    } else {
      offset = new HashMap<>(1);
      offset.put(OFFSET_ITER_TYPE_KEY, ShardIteratorType.fromValue(initialOffset));
    }

    return offset;
  }

  @Override
  public void close() {
    this.kinesisClient = null;
  }

}
