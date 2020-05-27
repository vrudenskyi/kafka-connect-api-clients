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
package com.mckesson.kafka.connect.aws;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.model.Record;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CloudwatchKinesisBuilder extends KinesisRecordBuilder {

  private static final Logger log = LoggerFactory.getLogger(CloudwatchKinesisBuilder.class);

  private ObjectMapper jsonMapper = new ObjectMapper();

  @Override
  public Collection<? extends SourceRecord> buildRecords(String topic, Map<String, Object> partition, Map<String, Object> offset, Headers headers, Record record) {

    List<SourceRecord> result = new ArrayList<>();

    byte[] data = new byte[record.getData().remaining()];
    record.getData().get(data);
    try {
      GZIPInputStream iis = new GZIPInputStream(new ByteArrayInputStream(data));
      JsonNode node = jsonMapper.readTree(iis);
      JsonNode dataNode = node.at("/logEvents");
      if (dataNode.isArray()) {
        for (JsonNode n : dataNode) {

          String logGroup = node.at("/logGroup").asText();
          String logStream = node.at("/logStream").asText();
          String subscriptionFilters = node.at("/subscriptionFilters").asText();

          SourceRecord sourceRecord = new SourceRecord(partition, offset, topic, null, Schema.STRING_SCHEMA, record.getPartitionKey(), Schema.STRING_SCHEMA, jsonMapper.writeValueAsString(n),
              Long.valueOf(record.getApproximateArrivalTimestamp().getTime()), headers);

          sourceRecord.headers().addString(HEADER_KINESIS_STREAM_NAME, partition.get(KinesisAPIClient.PARTITION_STREAM_NAME_KEY).toString());
          sourceRecord.headers().addString(HEADER_KINESIS_PARTITION_KEY, record.getPartitionKey());
          sourceRecord.headers().addString(HEADER_KINESIS_SEQUENCE_NUMBER, record.getSequenceNumber());
          sourceRecord.headers().addString("cloudwatch.logGroup", logGroup);
          sourceRecord.headers().addString("cloudwatch.logStream", logStream);
          sourceRecord.headers().addString("cloudwatch.subscriptionFilters", subscriptionFilters);
          result.add(sourceRecord);

        }
      } else {
        SourceRecord sourceRecord = new SourceRecord(partition, offset, topic, null, Schema.STRING_SCHEMA, record.getPartitionKey(), Schema.STRING_SCHEMA, jsonMapper.writeValueAsString(node),
            Long.valueOf(record.getApproximateArrivalTimestamp().getTime()));
        log.debug("Kinesis Record content is different from what it expected to be");
        sourceRecord.headers().addString(HEADER_KINESIS_STREAM_NAME, partition.get(KinesisAPIClient.PARTITION_STREAM_NAME_KEY).toString());
        sourceRecord.headers().addString(HEADER_KINESIS_PARTITION_KEY, record.getPartitionKey());
        sourceRecord.headers().addString(HEADER_KINESIS_SEQUENCE_NUMBER, record.getSequenceNumber());
        result.add(sourceRecord);
      }

    } catch (IOException e) {
      log.debug("Failed to read ecord content", e);
      return super.buildRecords(topic, partition, offset, headers, record);
    }
    return result;
  }

}
