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
package com.vrudenskyi.kafka.connect.gcs;

import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Blob.BlobSourceOption;

public class CSVBlobReader extends TextBlobReader {

  private static final Logger log = LoggerFactory.getLogger(CSVBlobReader.class);

  public static final String GCS_READER_CSV_DELIMETER_CONFIG = "gcs.reader.csv.delimiter";
  public static final String GCS_READER_CSV_DELIMETER_DEFAULT = ",";

  public static final String GCS_READER_CSV_FIRST_RECORD_HEADER_CONFIG = "gcs.reader.csv.firstRecordAsHeader";
  public static final Boolean GCS_READER_CSV_FIRST_RECORD_HEADER_DEFAULT = Boolean.FALSE;

  public static final String GCS_READER_CSV_QUOTE_CHAR_CONFIG = "gcs.reader.csv.quoteCharacter";
  public static final String GCS_READER_CSV_QUOTE_CHAR_DEFAULT = "\"";

  public static final String GCS_READER_CSV_RECORD_SEPARATOR_CONFIG = "gcs.reader.csv.recordSeparator";
  public static final String GCS_READER_CSV_RECORD_SEPARATOR_DEFAULT = "\r\n";

  public static final String GCS_READER_CSV_IGNORE_EMPTY_LINES_CONFIG = "gcs.reader.csv.ignoreEmptyLines";
  public static final Boolean GCS_READER_CSV_IGNORE_EMPTY_LINES_DEFAULT = Boolean.TRUE;

  public static final String GCS_READER_CSV_HEADER_CONFIG = "gcs.reader.csv.header";

  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(GCS_READER_CSV_DELIMETER_CONFIG, Type.STRING, GCS_READER_CSV_DELIMETER_DEFAULT, Importance.MEDIUM, "CSV Delimeter")
      .define(GCS_READER_CSV_FIRST_RECORD_HEADER_CONFIG, Type.BOOLEAN, GCS_READER_CSV_FIRST_RECORD_HEADER_DEFAULT, Importance.MEDIUM, "firstRecordAsHeader")
      .define(GCS_READER_CSV_QUOTE_CHAR_CONFIG, Type.STRING, GCS_READER_CSV_QUOTE_CHAR_DEFAULT, Importance.MEDIUM, "Quote char")
      .define(GCS_READER_CSV_RECORD_SEPARATOR_CONFIG, Type.STRING, GCS_READER_CSV_RECORD_SEPARATOR_DEFAULT, Importance.MEDIUM, "record separator")
      .define(GCS_READER_CSV_IGNORE_EMPTY_LINES_CONFIG, Type.BOOLEAN, GCS_READER_CSV_IGNORE_EMPTY_LINES_DEFAULT, Importance.MEDIUM, "ignoreEmptyLines")
      .define(GCS_READER_CSV_HEADER_CONFIG, Type.LIST, null, Importance.MEDIUM, "headers");

  private String delimeter;
  private Boolean firstRecordAsHeader;
  private String quoteCharacter;
  private String recordSeparator;
  private Boolean ignoreEmptyLines;
  private List<String> header;

  private CSVFormat csvFormat;

  @Override
  public void configure(Map<String, ?> configs) {
    super.configure(configs);
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);

    this.delimeter = conf.getString(GCS_READER_CSV_DELIMETER_CONFIG);
    this.firstRecordAsHeader = conf.getBoolean(GCS_READER_CSV_FIRST_RECORD_HEADER_CONFIG);
    this.quoteCharacter = conf.getString(GCS_READER_CSV_QUOTE_CHAR_CONFIG);
    this.recordSeparator = conf.getString(GCS_READER_CSV_RECORD_SEPARATOR_CONFIG);
    this.ignoreEmptyLines = conf.getBoolean(GCS_READER_CSV_IGNORE_EMPTY_LINES_CONFIG);
    this.header = conf.getList(GCS_READER_CSV_HEADER_CONFIG);

    csvFormat = CSVFormat.DEFAULT
        .withDelimiter(this.delimeter.charAt(0))
        .withQuote(this.quoteCharacter.charAt(0))
        .withRecordSeparator(recordSeparator)
        .withIgnoreEmptyLines(this.ignoreEmptyLines);

    if (this.firstRecordAsHeader) {
      csvFormat = csvFormat.withFirstRecordAsHeader();
    }

    if (this.header != null && header.size() > 0) {
      csvFormat = csvFormat.withHeader(header.toArray(new String[0]));
    }

  }

  @Override
  public Object read(Blob blob, Object offset, Collection<String> records, int itemsToPoll) throws IOException {
    
    if (blob == null) {
      return null;
    }

    ReadChannel readChannel = blob.reader(readOpts.toArray(new BlobSourceOption[0]));
    if (readChunkSize > 0) {
      readChannel.setChunkSize(readChunkSize);
    }
    try (Reader reader = Channels.newReader(readChannel, Charset.forName(charset).newDecoder(), bufferSize);
        CSVParser recs = csvFormat.parse(reader);) {

      for (CSVRecord r : recs) {
        Map<String, String> rMap = r.toMap();
        if (rMap.isEmpty()) {
          records.add(StringUtils.join(r, this.delimeter.charAt(0)));
        } else {
          records.add(JSON_MAPPER.writeValueAsString(rMap));
        }
      }
    }
    return null;
  }

}
