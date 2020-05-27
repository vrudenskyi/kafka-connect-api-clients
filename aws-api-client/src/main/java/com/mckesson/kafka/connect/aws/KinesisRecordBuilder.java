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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;

import com.amazonaws.services.kinesis.model.Record;

public class KinesisRecordBuilder implements SourceRecordBuilder<Record>, Configurable {

  public static final String HEADER_KINESIS_PARTITION_KEY = "kinesis.partition.key";
  public static final String HEADER_KINESIS_SEQUENCE_NUMBER = "kinesis.sequence.number";
  public static final String HEADER_KINESIS_STREAM_NAME = "kinesis.stream.name";

  @Override
  public Collection<? extends SourceRecord> buildRecords(String topic, Map<String, Object> partition, Map<String, Object> offset, Headers headers, Record record) {
    byte[] recordData = new byte[record.getData().remaining()];
    record.getData().get(recordData);

    SourceRecord sourceRecord = new SourceRecord(partition, offset, topic, null, Schema.STRING_SCHEMA, record.getPartitionKey(), Schema.BYTES_SCHEMA, recordData, Long.valueOf(record.getApproximateArrivalTimestamp().getTime()), headers);
    sourceRecord.headers().addString(HEADER_KINESIS_STREAM_NAME, partition.get(KinesisAPIClient.PARTITION_STREAM_NAME_KEY).toString());
    sourceRecord.headers().addString(HEADER_KINESIS_PARTITION_KEY, record.getPartitionKey());
    sourceRecord.headers().addString(HEADER_KINESIS_SEQUENCE_NUMBER, record.getSequenceNumber());

    return Arrays.asList(sourceRecord);

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }

}
