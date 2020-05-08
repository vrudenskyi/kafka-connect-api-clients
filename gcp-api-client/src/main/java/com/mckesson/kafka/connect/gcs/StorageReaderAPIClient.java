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

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobField;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.StorageOptions;
import com.mckesson.kafka.connect.source.APIClientException;
import com.mckesson.kafka.connect.source.PollableAPIClient;

public class StorageReaderAPIClient implements PollableAPIClient {

  private static final Logger log = LoggerFactory.getLogger(StorageReaderAPIClient.class);

  public static final String GCP_CREDENTIALS_FILE_PATH_CONFIG = "gcp.credentials.file.path";
  public static final String GCP_CREDENTIALS_JSON_CONFIG = "gcp.credentials.json";

  public static final String GCS_USER_PROJECT_CONFIG = "gcs.userProject";
  public static final String GCS_USER_PROJECT_DEFAULT = null;

  public static final String GCS_BUCKET_NAME_CONFIG = "gcs.bucket.name";

  public static final String GCS_BLOB_PATTERN_CONFIG = "gcs.blob.pattern";
  public static final String GCS_BLOB_PATTERN_DEFAULT = null;

  public static final String GCS_BLOB_PREFIX_LIST_CONFIG = "gcs.blob.prefix.list";

  public static final String GCS_BLOB_PARTITIONS_ENABLED_CONFIG = "gcs.blob.partitions.enabled";
  public static final Boolean GCS_BLOB_PARTITIONS_ENABLED_DEFAULT = false;
  public static final String GCS_BLOB_PARTITIONS_DELIMITER_CONFIG = "gcs.blob.partitions.delimiter";
  public static final String GCS_BLOB_PARTITIONS_DELIMITER_DEFAULT = "/";
  public static final String GCS_BLOB_PARTITIONS_DEPTH_CONFIG = "gcs.blob.partitions.depth";
  public static final Integer GCS_BLOB_PARTITIONS_DEPTH_DEFAULT = 1;

  public static final String GCS_BLOB_MOVE_PROCESSED_CONFIG = "gcs.blob.moveProcessed";
  public static final Boolean GCS_BLOB_MOVE_PROCESSED_DEFAULT = false;
  public static final String GCS_BLOB_MOVE_PROCESSED_PREFIX_CONFIG = "gcs.blob.moveProcessed.prefix";
  public static final String GCS_BLOB_MOVE_PROCESSED_BUCKET_CONFIG = "gcs.blob.moveProcessed.bucket";

  public static final String GCS_BLOB_LIST_PAGESIZE_CONFIG = "gcs.blob.list.pageSize";
  public static final int GCS_BLOB_LIST_PAGESIZE_DEFAULT = 1000;
  public static final String GCS_BLOB_LIST_MAX_QUEUE_SIZE_CONFIG = "gcs.blob.list.maxQueueSize";
  public static final int GCS_BLOB_LIST_MAX_QUEUE_SIZE_DEFAULT = 10000;

  public static final String GCS_READER_CLASS_CONFIG = "gcs.reader.class";
  public static final Class<? extends BlobReader> GCS_READER_CLASS_DEFAULT = TextBlobReader.class;

  private static final String PARTITION_BUCKET_NAME_KEY = "__bucketName";
  private static final String PARTITION_BLOB_PREFIX_KEY = "__blobPrefix";

  private static final String OFFSET_CURRENT_BLOB_NAME_KEY = "__currentBlob";
  private static final String OFFSET_LAST_BLOB_TIME_KEY = "__lastLastBlobTime";
  private static final String OFFSET_CURRENT_BLOB_OFFSET_KEY = "__currentBlobOffset";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(GCP_CREDENTIALS_FILE_PATH_CONFIG, Type.STRING, null, Importance.HIGH, "The path to the GCP credentials file")
      .define(GCP_CREDENTIALS_JSON_CONFIG, Type.STRING, null, Importance.HIGH, "GCP JSON credentials")
      .define(GCS_BUCKET_NAME_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "GCS Bucket Name")
      .define(GCS_BLOB_PATTERN_CONFIG, Type.STRING, GCS_BLOB_PATTERN_DEFAULT, Importance.HIGH, "Regexp filter on blob names to read. Default: '.*'")
      .define(GCS_READER_CLASS_CONFIG, Type.CLASS, GCS_READER_CLASS_DEFAULT, Importance.MEDIUM, "BlobReader class impl")

      .define(GCS_BLOB_PREFIX_LIST_CONFIG, Type.LIST, Arrays.asList(""), Importance.MEDIUM, "List of prefixes for blobs")

      .define(GCS_BLOB_PARTITIONS_ENABLED_CONFIG, Type.BOOLEAN, GCS_BLOB_PARTITIONS_ENABLED_DEFAULT, Importance.MEDIUM, "Enable name based partitioneing")
      .define(GCS_BLOB_PARTITIONS_DELIMITER_CONFIG, Type.STRING, GCS_BLOB_PARTITIONS_DELIMITER_DEFAULT, Importance.MEDIUM, "Delimeter in name to use as delimeter")
      .define(GCS_BLOB_PARTITIONS_DEPTH_CONFIG, Type.INT, GCS_BLOB_PARTITIONS_DEPTH_DEFAULT, Importance.MEDIUM, "Delimeter in name to use as delimeter")

      .define(GCS_BLOB_MOVE_PROCESSED_CONFIG, Type.BOOLEAN, GCS_BLOB_MOVE_PROCESSED_DEFAULT, Importance.MEDIUM, "Enable move of processed blobs")
      .define(GCS_BLOB_MOVE_PROCESSED_PREFIX_CONFIG, Type.STRING, "", Importance.MEDIUM, "Prefix to be added to processed partitions (if enabled)")
      .define(GCS_BLOB_MOVE_PROCESSED_BUCKET_CONFIG, Type.STRING, null, Importance.MEDIUM, "Name of bucket where to move partitions")

      .define(GCS_USER_PROJECT_CONFIG, Type.STRING, GCS_USER_PROJECT_DEFAULT, Importance.LOW, "userProject for operations")

      .define(GCS_BLOB_LIST_PAGESIZE_CONFIG, Type.INT, GCS_BLOB_LIST_PAGESIZE_DEFAULT, Importance.LOW, "Pagesize for list operation")
      .define(GCS_BLOB_LIST_MAX_QUEUE_SIZE_CONFIG, Type.INT, GCS_BLOB_LIST_MAX_QUEUE_SIZE_DEFAULT, Importance.LOW, "Max number of blobs to keep in queue during poll");

  private BlobField[] blobFields = new BlobField[] {
      BlobField.NAME, BlobField.SIZE, BlobField.TIME_CREATED, BlobField.CONTENT_TYPE, BlobField.UPDATED
  };

  private String bucketName;
  private Pattern blobNamePattern;

  private List<String> blobPrefixes;

  private boolean partitionsEnabled;
  private String partitionsDelimiter;
  private int partitionsDepth;

  private boolean processedMove;
  private String processedBucketName;
  private String processedPrefix;

  private int blobListPageSize;
  private int maxQueueSize;
  private String userProject;

  private PriorityQueue<Blob> blobsQueue;
  private BlobReader<Object> reader;
  private Storage storage;

  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);

    this.bucketName = conf.getString(GCS_BUCKET_NAME_CONFIG);
    String pattern = conf.getString(GCS_BLOB_PATTERN_CONFIG);
    if (StringUtils.isNotBlank(pattern)) {
      this.blobNamePattern = Pattern.compile(pattern);
    }

    this.blobPrefixes = conf.getList(GCS_BLOB_PREFIX_LIST_CONFIG);

    this.partitionsEnabled = conf.getBoolean(GCS_BLOB_PARTITIONS_ENABLED_CONFIG);
    this.partitionsDelimiter = conf.getString(GCS_BLOB_PARTITIONS_DELIMITER_CONFIG);
    this.partitionsDepth = conf.getInt(GCS_BLOB_PARTITIONS_DEPTH_CONFIG);

    if (partitionsEnabled && (StringUtils.isBlank(this.partitionsDelimiter) || partitionsDepth <= 0)) {
      throw new ConnectException("Name based partitioning is enabled but depth and delimeter not properly configured");
    }

    this.processedMove = conf.getBoolean(GCS_BLOB_MOVE_PROCESSED_CONFIG);
    this.processedBucketName = conf.getString(GCS_BLOB_MOVE_PROCESSED_BUCKET_CONFIG);
    this.processedPrefix = conf.getString(GCS_BLOB_MOVE_PROCESSED_PREFIX_CONFIG);

    if (processedMove && StringUtils.isBlank(this.processedBucketName) && StringUtils.isBlank(this.processedPrefix)) {
      throw new ConnectException("Processed move is enabled but bucket and prefix not properly configured");
    }

    this.blobListPageSize = conf.getInt(GCS_BLOB_LIST_PAGESIZE_CONFIG);
    this.maxQueueSize = conf.getInt(GCS_BLOB_LIST_MAX_QUEUE_SIZE_CONFIG);

    this.userProject = conf.getString(GCS_USER_PROJECT_CONFIG);
    blobsQueue = new PriorityQueue<>(new Comparator<Blob>() {
      @Override
      public int compare(Blob o1, Blob o2) {
        if (o1 == null && o2 == null)
          return 0;
        if (o1 != null && o2 == null)
          return 1;
        if (o1 == null && o2 != null)
          return -1;
        return o1.getCreateTime().compareTo(o2.getCreateTime());
      }
    });

    reader = conf.getConfiguredInstance(GCS_READER_CLASS_CONFIG, BlobReader.class);

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

      this.storage = StorageOptions.newBuilder()
          .setCredentials(GoogleCredentials.fromStream(credsIS))
          .build()
          .getService();

    } catch (ConnectException e) {
      throw e;
    } catch (Exception e) {
      throw new ConnectException("Failed to create SecurityCenterClient", e);
    }

  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {

    if (!offset.containsKey(OFFSET_CURRENT_BLOB_NAME_KEY) && blobsQueue.size() == 0) {
      String bucketName = (String) partition.get(PARTITION_BUCKET_NAME_KEY);
      String blobPrefix = (String) partition.get(PARTITION_BLOB_PREFIX_KEY);

      Long lastBlobTime = (Long) offset.getOrDefault(OFFSET_LAST_BLOB_TIME_KEY, 0L);
      log.debug("List blobs for bucket: {} with prefix: {}", bucketName, blobPrefix);
      Page<Blob> listResult = listBlobs(getBucket(bucketName), blobPrefix);

      for (Blob b : listResult.iterateAll()) {
        String name = b.getName();
        if (this.processedMove && StringUtils.isNotBlank(this.processedPrefix) && name.startsWith(this.processedPrefix)) {
          //skip already processed file
          continue;
        }

        if ((blobNamePattern == null || blobNamePattern.matcher(b.getName()).matches()) && (lastBlobTime == 0L || b.getCreateTime() > lastBlobTime)) {
          blobsQueue.add(b);
        }
        if (blobsQueue.size() >= this.maxQueueSize) {
          log.warn("Blob's queue reached its max size {}, some file can be potentially missed. Consider to narrow partitions or increase queue size");
          break;
        }
      }
      log.debug("Found {} blobs available.", blobsQueue.size());
    }

    boolean done = blobsQueue.size() == 0 && !offset.containsKey(OFFSET_CURRENT_BLOB_NAME_KEY);
    List<SourceRecord> pollResult = new ArrayList<>();
    while (!done) {
      Blob currentBlob = null;
      Object currentBlobOffset = null;
      if (offset.containsKey(OFFSET_CURRENT_BLOB_NAME_KEY)) {
        String currentBlobName = (String) offset.get(OFFSET_CURRENT_BLOB_NAME_KEY);
        String bucketName = (String) partition.get(PARTITION_BUCKET_NAME_KEY);
        log.debug("Resume Blob:  {}@{}", currentBlobName, bucketName);
        currentBlob = getBlob(getBucket(bucketName), currentBlobName);
        currentBlobOffset = offset.get(OFFSET_CURRENT_BLOB_OFFSET_KEY);
      } else {
        currentBlob = blobsQueue.poll();
        if (currentBlob == null) {
          log.debug("No more blobs in queue. Exit from poll");
          break;
        }
        log.debug("Start Blob:  {}@{}", currentBlob.getName(), currentBlob.getBucket());
        offset.put(OFFSET_CURRENT_BLOB_NAME_KEY, currentBlob.getName());
      }

      List<Object> dataRecs = new ArrayList<>();

      try {
        if (currentBlob != null) {
          currentBlobOffset = reader.read(currentBlob, currentBlobOffset, dataRecs, itemsToPoll - pollResult.size());
          log.debug("Loaded {} records from {}", dataRecs.size(), currentBlob.getBlobId());
        }
      } catch (IOException e) {
        throw new ConnectException("Failed to read:" + currentBlob, e);
      }

      pollResult.addAll(createRecords(dataRecs, partition, offset, topic, currentBlob.getBlobId()));

      if (currentBlobOffset != null) {
        //update blob offset
        offset.put(OFFSET_CURRENT_BLOB_OFFSET_KEY, currentBlobOffset);
      } else {
        //clear offset to peek next from queue 
        offset.remove(OFFSET_CURRENT_BLOB_OFFSET_KEY);
        offset.remove(OFFSET_CURRENT_BLOB_NAME_KEY);

        //move currentBlob if enabled
        //we do not need to track created time when we move processed files
        if (this.processedMove) {
          moveBlob(currentBlob);
        } else {
          offset.put(OFFSET_LAST_BLOB_TIME_KEY, currentBlob.getCreateTime());
        }
      }

      if (pollResult.size() >= itemsToPoll || blobsQueue.size() == 0) {
        done = true;
      }

    }

    return pollResult;
  }

  private void moveBlob(Blob currentBlob) {
    if (currentBlob == null) {
      return;
    }
    if (StringUtils.isNotBlank(this.processedBucketName)) {
      log.debug("Move processed blob to bucket: {}/{}/{}", this.processedBucketName, this.processedPrefix, currentBlob.getName());
      currentBlob.copyTo(this.processedBucketName, processedPrefix + currentBlob.getName());
      currentBlob.delete();
    } else {
      log.debug("Rename processed blob with prefix: {}/{}", this.processedPrefix, currentBlob.getName());
      currentBlob.copyTo(BlobId.of(currentBlob.getBucket(), this.processedPrefix + currentBlob.getName()));
      currentBlob.delete();
    }

  }

  private Collection<? extends SourceRecord> createRecords(List<Object> dataRecs, Map<String, Object> partition, Map<String, Object> offset, String topic, BlobId blobId) {
    List<SourceRecord> recs = new ArrayList<>(dataRecs.size());
    for (Object r : dataRecs) {
      SourceRecord srcRec = new SourceRecord(partition, offset, topic, null, r);
      srcRec.headers().addString("gcs.bucketName", blobId.getBucket());
      srcRec.headers().addString("gcs.blobName", blobId.getName());
      if (StringUtils.isNotBlank(userProject)) {
        srcRec.headers().addString("gcs.userProject", userProject);
      }
      recs.add(srcRec);
    }

    return recs;
  }

  private Page<Blob> listBlobs(Bucket bucket, String blobPrefix) {
    List<BlobListOption> listOpts = new ArrayList<>(Arrays.asList(BlobListOption.pageSize(blobListPageSize), BlobListOption.fields(blobFields)));

    if (StringUtils.isNotBlank(userProject)) {
      listOpts.add(BlobListOption.userProject(userProject));
    }
    if (StringUtils.isNotBlank(blobPrefix)) {
      listOpts.add(BlobListOption.prefix(blobPrefix));
    }

    Page<Blob> listResult = bucket.list(listOpts.toArray(new BlobListOption[0]));
    return listResult;
  }

  private Bucket getBucket(String bucketName) {
    List<BucketGetOption> getOpts = new ArrayList<>();
    if (StringUtils.isNotBlank(userProject)) {
      getOpts.add(BucketGetOption.userProject(userProject));
    }
    Bucket bucket = storage.get(bucketName, getOpts.toArray(new BucketGetOption[0]));
    return bucket;
  }

  private Blob getBlob(Bucket bucket, String blobName) {
    List<BlobGetOption> getOpts = new ArrayList<>();
    if (StringUtils.isNotBlank(userProject)) {
      getOpts.add(BlobGetOption.userProject(userProject));
    }
    getOpts.add(BlobGetOption.fields(blobFields));
    Blob blob = bucket.get(blobName, getOpts.toArray(new BlobGetOption[0]));
    return blob;
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {

    Set<String> prefixes;
    if (this.partitionsEnabled) {
      log.debug("Named based partitioniong is enabled with: delimiter={}, depth={}", this.partitionsDelimiter, partitionsDepth);
      prefixes = new HashSet<>();
      for (String prefix : this.blobPrefixes) {
        Page<Blob> listResult = listBlobs(getBucket(bucketName), prefix);
        int counter = 0;
        for (Blob b : listResult.iterateAll()) {
          String name = b.getName();
          if (this.processedMove && StringUtils.isNotBlank(this.processedPrefix) && name.startsWith(this.processedPrefix)) {
            //skip processed file
            continue;
          }
          int idx = StringUtils.ordinalIndexOf(name, this.partitionsDelimiter, this.partitionsDepth);
          if (idx > 0) {
            prefixes.add(name.substring(0, idx));
          } else {
            prefixes.add(name);
          }
          counter++;
        }
      }
      log.debug("Found {} partitioned prefixes.", prefixes.size());
    } else {
      if (this.blobPrefixes == null || this.blobPrefixes.size() == 0) {
        log.debug("Create ONE partition");
        prefixes = new HashSet<>(Arrays.asList(""));
      } else {
        log.debug("Create partitions for prefixes: {}", this.blobPrefixes);
        prefixes = new HashSet<>(this.blobPrefixes);
      }
    }

    List<Map<String, Object>> partitions = new ArrayList<>(prefixes.size());
    for (String prefix : prefixes) {
      Map<String, Object> partition = new HashMap<>();
      partition.put(PARTITION_BUCKET_NAME_KEY, bucketName);
      partition.put(PARTITION_BLOB_PREFIX_KEY, prefix);
      partitions.add(partition);
    }
    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    Map<String, Object> offset = new HashMap<>(2);
    return offset;
  }

  @Override
  public void close() {
    blobsQueue.clear();
  }

}
