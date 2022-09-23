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

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;

public interface SourceRecordBuilder<T> {

  public Collection<? extends SourceRecord> buildRecords(String topic, Map<String, Object> partition, Map<String, Object> offset, Headers headers, T data);

}
