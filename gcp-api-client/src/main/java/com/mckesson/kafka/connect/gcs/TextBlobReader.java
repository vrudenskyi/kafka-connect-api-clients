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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.rmi.ConnectException;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Blob.BlobSourceOption;

public class TextBlobReader extends BlobReader<String> {

  private static final Logger log = LoggerFactory.getLogger(TextBlobReader.class);

  private static final String GCS_READER_BUFFERSIZE_CONFIG = "gcs.reader.buffer.size";
  private static final int GCS_READER_BUFFERSIZE_DEFAULT = -1; //implementation defaults

  private static final String GCS_READER_TEXT_CHARSET_CONFIG = "gcs.reader.text.charset";
  private static final String GCS_READER_TEXT_CHARSET_DEFAULT = "UTF-8";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(GCS_READER_BUFFERSIZE_CONFIG, Type.INT, GCS_READER_BUFFERSIZE_DEFAULT, Importance.LOW, "Buffer size to allocate. Default: depends on impl")
      .define(GCS_READER_TEXT_CHARSET_CONFIG, Type.STRING, GCS_READER_TEXT_CHARSET_DEFAULT, Importance.LOW, "Reader charset, default UTF-8");

  protected String charset;
  protected int bufferSize;

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.charset = conf.getString(GCS_READER_TEXT_CHARSET_CONFIG);
    this.bufferSize = conf.getInt(GCS_READER_BUFFERSIZE_CONFIG);
  }

  @Override
  public Object read(Blob blob, Object offset, Collection<String> records, int itemsToPoll) throws IOException {

    ReadChannel readChannel;
    if (offset != null && StringUtils.isNotBlank(offset.toString())) {
      RestorableState<ReadChannel> state;
      try {
        state = stateFromString((String) offset);
      } catch (Exception e) {
        throw new ConnectException("Failed to restore state of ReadChannel", e);
      }
      readChannel = state.restore();
    } else {
      readChannel = blob.reader(readOpts.toArray(new BlobSourceOption[0]));
    }

    if (readChunkSize > 0) {
      readChannel.setChunkSize(readChunkSize);
    }

    String channelState = null;
    try (Reader reader = Channels.newReader(readChannel, Charset.forName(charset).newDecoder(), bufferSize);
        BufferedReader br = new BufferedReader(reader);) {

      String line = br.readLine();
      boolean readerClosed = false;
      while (line != null) {
        if (StringUtils.isNotBlank(line)) {
          records.add(line);
        }

        try {
          line = br.readLine();
        } catch (Exception e) {
          if (!readerClosed) {
            //reader is not closed but exception
            throw e;
          }
          //we know reader is closed. the exception is expected
          //hide exception because it was reading of the  buffered data
          //log.trace("buffer reading complete", e);
          break;
        }

        if (records.size() >= itemsToPoll && !readerClosed) {
          RestorableState<ReadChannel> cnlState = readChannel.capture();
          log.debug("Channel state: {}", cnlState);
          //quite stupid made all states of implementation private.
          //if endOfStream==true => no need to save channelstate
          try {
            Object o = FieldUtils.readField(cnlState, "endOfStream", true);
            if (!Boolean.valueOf(o.toString())) {
              channelState = stateToString(cnlState);
            }
          } catch (Exception e) {
            log.warn("Unexpected type of state (without endOfStream property)", e);
            channelState = stateToString(cnlState);
          }
          //closed underlying reader
          //note:  the reader is closed but since we have BufferedReader  will keep reading until all buffered data read
          reader.close();
          readerClosed = true;
         
        }
      }
    }
    return channelState;
  }

  private RestorableState<ReadChannel> stateFromString(String s) throws Exception {
    byte[] data = Base64.getDecoder().decode(s);
    ObjectInputStream ois = new ObjectInputStream(
        new ByteArrayInputStream(data));
    RestorableState<ReadChannel> state = (RestorableState<ReadChannel>) ois.readObject();
    ois.close();
    return state;
  }

  private String stateToString(RestorableState<ReadChannel> state) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(state);
    oos.close();
    return Base64.getEncoder().encodeToString(baos.toByteArray());
  }

}
