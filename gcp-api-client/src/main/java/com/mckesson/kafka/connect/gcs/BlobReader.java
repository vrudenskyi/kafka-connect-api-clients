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
package com.mckesson.kafka.connect.gcs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Blob.BlobSourceOption;

public abstract class BlobReader<T> implements Configurable {

  private static final String GCS_READER_CHUNKSIZE_CONFIG = "gcs.reader.chunk.size";
  private static final int GCS_READER_CHUNKSIZE_DEFAULT = 256 * 1024; //256k

  private static final String GCS_READER_DECRYPTION_KEY_CONFIG = "gcs.reader.decryption.key";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(StorageReaderAPIClient.GCS_USER_PROJECT_CONFIG, Type.STRING, StorageReaderAPIClient.GCS_USER_PROJECT_DEFAULT, Importance.MEDIUM, "userProject for operations")
      .define(GCS_READER_CHUNKSIZE_CONFIG, Type.INT, GCS_READER_CHUNKSIZE_DEFAULT, Importance.LOW, "Chunk size  to reade from blod")
      .define(GCS_READER_DECRYPTION_KEY_CONFIG, Type.STRING, null, Importance.LOW, "customer-supplied AES256 key, encoded in base64");

  protected List<BlobSourceOption> readOpts = new ArrayList<>();
  protected String topic;
  protected int readChunkSize;

  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);

    String userProject = conf.getString(StorageReaderAPIClient.GCS_USER_PROJECT_CONFIG);
    if (StringUtils.isNotBlank(userProject)) {
      readOpts.add(BlobSourceOption.userProject(userProject));
    }

    String decryptionKey = conf.getString(GCS_READER_DECRYPTION_KEY_CONFIG);
    if (StringUtils.isNotBlank(decryptionKey)) {
      readOpts.add(BlobSourceOption.decryptionKey(decryptionKey));
    }

    this.readChunkSize = conf.getInt(GCS_READER_CHUNKSIZE_CONFIG);

  }

  /**
   * 
   * 
   * @param blob
   * @param offset
   * @param records
   * @param itemsToPoll
   * @return
   */
  public abstract Object read(Blob blob, Object offset, Collection<T> dataRecords, int itemsToPoll) throws IOException;

}
