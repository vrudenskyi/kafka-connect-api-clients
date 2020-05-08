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
package com.mckesson.kafka.connect.incapsula;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.InflaterInputStream;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mckesson.kafka.connect.http.BasicAuthenticator;
import com.mckesson.kafka.connect.source.APIClientException;
import com.mckesson.kafka.connect.source.PollableAPIClient;
import com.mckesson.kafka.connect.utils.OkHttpClientConfig;
import com.mckesson.kafka.connect.utils.SslUtils;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 *Client for API:  * https://docs.imperva.com/bundle/cloud-application-security/page/settings/log-integration.htm
 * 
 */


public class IncapulaLogsPollableClient implements PollableAPIClient {

  private static final Logger log = LoggerFactory.getLogger(IncapulaLogsPollableClient.class);

  public static final String CONF_PREFIX = "imperva.";
  public static final String ACCOUNTS_CONFIG = CONF_PREFIX + "accounts";

  public static final String BASEURL_CONFIG = "baseUrl";
  public static final String APPID_CONFIG = "appId";
  public static final String APPKEY_CONFIG = "appKey";
  public static final String INITIAL_FILE_CONFIG = "initialFile";
  public static final String PUBLIC_KEY_PREFIX_CONFIG = "publicKeyPrefix";
  public static final String PUBLIC_KEY_PREFIX_DEFAULT = "imperva_";

  public static final ConfigDef ACCOUNT_CONF_DEF = new ConfigDef()
      .define(BASEURL_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Base url for log location")
      .define(APPID_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Application Id")
      .define(APPKEY_CONFIG, ConfigDef.Type.PASSWORD, null, ConfigDef.Importance.HIGH, "Key")
      .define(INITIAL_FILE_CONFIG, ConfigDef.Type.STRING, "----------", ConfigDef.Importance.LOW, "Start reading with filename")
      .define(PUBLIC_KEY_PREFIX_CONFIG, ConfigDef.Type.STRING, PUBLIC_KEY_PREFIX_DEFAULT, ConfigDef.Importance.LOW, "Prefix for publicKeyId as it stored in keystore")
      .withClientSslSupport();

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(ACCOUNTS_CONFIG, ConfigDef.Type.LIST, new ArrayList<>(1), ConfigDef.Importance.HIGH, "List of configured accounts");

  public static final String PARTITION_APP_ID_KEY = "__APPID_ID";
  public static final String PARTITION_ENDPOINT_KEY = "__ENDPOINT";
  public static final String OFFSET_LAST_FILE_KEY = "__LASTFILE";
  public static final String E_KEY_PREFIX = "__KEY_PREFIX";
  public static final String E_KEYSTORE = "__KEYSTORE";
  public static final String E_KEY_PASS = "__KEY_PASS";

  Map<Map<String, Object>, OkHttpClient> httpClients;
  List<Map<String, Object>> partitions;
  Map<Map<String, Object>, Map<String, Object>> initialOffsets;
  Map<Map<String, Object>, List<String>> indexFiles;
  Map<Map<String, Object>, Map<String, Object>> encryptionConfs;

  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig clientConf = new SimpleConfig(CONFIG_DEF, configs);

    List<String> accounts = clientConf.getList(ACCOUNTS_CONFIG);
    if (accounts.size() == 0) {
      accounts.add("___default");
    }

    SimpleConfig defaultAccountConf = new SimpleConfig(ACCOUNT_CONF_DEF, clientConf.originalsWithPrefix(CONF_PREFIX));
    String defaultUrl = defaultAccountConf.getString(BASEURL_CONFIG);
    String defaultId = defaultAccountConf.getString(APPID_CONFIG);
    Password defaultKey = defaultAccountConf.getPassword(APPKEY_CONFIG);
    String defaultInitialFile = defaultAccountConf.getString(INITIAL_FILE_CONFIG);

    //encryption specific props
    String defaultKeyPrefix = defaultAccountConf.getString(PUBLIC_KEY_PREFIX_CONFIG);
    String defaultKeystoreType = defaultAccountConf.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
    String defaultKeystoreLocation = defaultAccountConf.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
    Password defaultKeystorePass = defaultAccountConf.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
    Password defaultKeyPass = defaultAccountConf.getPassword(SslConfigs.SSL_KEY_PASSWORD_CONFIG);

    httpClients = new HashMap<>(accounts.size());
    partitions = new ArrayList<>(accounts.size());
    initialOffsets = new HashMap<>(accounts.size());
    indexFiles = new HashMap<>(accounts.size());
    encryptionConfs = new HashMap<>(accounts.size());
    for (String account : accounts) {
      Map<String, Object> accConfigs = clientConf.originalsWithPrefix(ACCOUNTS_CONFIG + "." + account + ".");

      Map<String, Object> accPartition = new HashMap<>(2);
      accPartition.put(PARTITION_APP_ID_KEY, accConfigs.getOrDefault(APPID_CONFIG, defaultId));
      accPartition.put(PARTITION_ENDPOINT_KEY, accConfigs.getOrDefault(BASEURL_CONFIG, defaultUrl));

      Map<String, Object> partition = Collections.unmodifiableMap(accPartition);
      partitions.add(partition);

      Map<String, Object> initialOffset = new HashMap<>(1);
      initialOffset.put(OFFSET_LAST_FILE_KEY, accConfigs.getOrDefault(INITIAL_FILE_CONFIG, defaultInitialFile));
      initialOffsets.put(partition, initialOffset);

      BasicAuthenticator accAuth = new BasicAuthenticator((String) accConfigs.getOrDefault(APPID_CONFIG, defaultId), new Password((String) accConfigs.getOrDefault(APPKEY_CONFIG, defaultKey == null ? "" : defaultKey.value())));
      OkHttpClient httpClient;
      try {
        httpClient = OkHttpClientConfig.buildClient(configs, accAuth);
      } catch (Exception e) {
        throw new ConnectException("Failed to configure httpClient for account: " + account, e);
      }
      httpClients.put(partition, httpClient);

      //configure encryptionConfs
      Map<String, Object> encryptionConf = new HashMap<>();
      encryptionConf.put(E_KEY_PREFIX, accConfigs.getOrDefault(PUBLIC_KEY_PREFIX_CONFIG, defaultKeyPrefix));

      encryptionConf.put(E_KEY_PASS, new Password((String) accConfigs.getOrDefault(SslConfigs.SSL_KEY_PASSWORD_CONFIG, defaultKeyPass == null ? "" : defaultKeyPass.value())));

      try {
        KeyStore ks = SslUtils.loadKeyStore(
            (String) accConfigs.getOrDefault(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, defaultKeystoreType),
            (String) accConfigs.getOrDefault(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, defaultKeystoreLocation),
            new Password((String) accConfigs.getOrDefault(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, defaultKeystorePass == null ? "" : defaultKeystorePass.value())));
        encryptionConf.put(E_KEYSTORE, ks);
      } catch (Exception e) {
        throw new ConnectException("Faile dto load keystore for account: " + account, e);
      }
      encryptionConfs.put(partition, encryptionConf);

    }

  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {

    List<SourceRecord> pollResult = new ArrayList<>(itemsToPoll);
    boolean partitionDone = false;
    while (!partitionDone) {

      String lastFile = (String) offset.get(OFFSET_LAST_FILE_KEY);
      List<String> indexFile = getIndexFile(partition);

      int lastFileIdx = indexFile.lastIndexOf(lastFile);
      if (indexFile.size() == 0 || lastFileIdx == indexFile.size() - 1) {
        log.debug("Empty index file or all files read from index file '{}/logs.index' skip, new index file will be downloaded with the next poll", partition.get(PARTITION_ENDPOINT_KEY));
        indexFiles.remove(partition);
        partitionDone = true;
      } else {
        String newFile = indexFile.get(lastFileIdx + 1);
        List<SourceRecord> recsFormFile = createRecords(topic, partition, offset, newFile, readFromFile(partition, newFile));
        log.debug("added {} records from file:{}/{} of cliendID: {}", recsFormFile.size(), partition.get(PARTITION_ENDPOINT_KEY), newFile, partition.get(PARTITION_APP_ID_KEY));
        pollResult.addAll(recsFormFile);
        partitionDone = pollResult.size() >= itemsToPoll;
        offset.put(OFFSET_LAST_FILE_KEY, newFile);
      }

    }

    return pollResult;
  }

  private List<Object> readFromFile(Map<String, Object> partition, String fileName) throws APIClientException {

    List<Object> dataList = new ArrayList<>();

    Request.Builder requestBuilder = new Request.Builder()
        .url(partition.get(PARTITION_ENDPOINT_KEY) + "/" + fileName)
        .get();
    OkHttpClient httpClient = httpClients.get(partition);
    Request request = requestBuilder.build();

    try (Response response = httpClient.newCall(request).execute()) { //TODO: implement async call

      if (!response.isSuccessful()) {
        if (HttpStatus.SC_NOT_FOUND == response.code()) {
          log.warn("File not found: {}", response.request().url().toString());
          return Collections.emptyList();
        }
        throw new APIClientException("Unexpected code: " + response);
      }

      InputStream dataIS = new ByteArrayInputStream(response.body().bytes());

      //scroll until delimeter
      int ch;
      int hdrPos = 0;
      char[] hdrDelimeter = "|==|\n".toCharArray();
      StringBuilder headerData = new StringBuilder();

      while ((ch = dataIS.read()) != -1) {
        if (ch == hdrDelimeter[hdrPos]) {
          hdrPos++;
        } else {
          hdrPos = 0;
        }
        if (hdrPos == hdrDelimeter.length)
          break;
        else
          headerData.append((char) ch);
      }

      Map<String, String> hdrsMap = parseHeader(headerData.toString());
      BufferedReader logLinesReader;
      if (hdrsMap.containsKey("key")) {
        byte[] aesKeyEncrypted = Base64.getDecoder().decode(hdrsMap.get("key"));
        String checksum = hdrsMap.get("checksum");
        String publicKeyId = hdrsMap.get("publicKeyId");
        log.debug("file: {} is encrypted with publicKeyId: {}", fileName, publicKeyId);
        String publicKeyAlias = encryptionConfs.get(partition).get(E_KEY_PREFIX) + publicKeyId;
        KeyStore ks = (KeyStore) encryptionConfs.get(partition).get(E_KEYSTORE);
        if (ks == null) {
          throw new APIClientException("Keystore not configured for encrypted logs:" + partition);
        }
        Password keyPass = (Password) encryptionConfs.get(partition).get(E_KEY_PASS);
        byte[] aesKey;
        try {
          aesKey = CryptUtils.rsaDecrypt(aesKeyEncrypted,
              ks,
              publicKeyAlias,
              keyPass.value().toCharArray(),
              "RSA/ECB/PKCS1Padding");
        } catch (Exception e) {
          throw new APIClientException("Failed to decrypt AES key from header", e);
        }

        byte[] decryptedData = null;
        try {
          byte[] encryptedData = IOUtils.toByteArray(dataIS);
          if (encryptedData != null && encryptedData.length > 0) {
            decryptedData = CryptUtils.aesDecrypt(encryptedData, Base64.getDecoder().decode(aesKey), "AES/CBC/PKCS5PADDING", new byte[16]);
          }
        } catch (Exception e) {
          throw new APIClientException("Failed to decrypt data", e);
        }

        if (decryptedData == null || decryptedData.length == 0) {
          log.debug("No data after decryption. returning empty list");
          return Collections.emptyList();
        }

        InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(decryptedData));
        log.debug("InflaterInputStream: ");

        logLinesReader = new BufferedReader(new InputStreamReader(iis));

      } else {
        log.debug("file: {} is NOT encrypted.", fileName);
        if (dataIS.available() == 0) {
          log.debug("No data available in file: {} of partition: {}", fileName, partition);
          return Collections.emptyList();
        }
        InflaterInputStream iis = new InflaterInputStream(dataIS);
        logLinesReader = new BufferedReader(new InputStreamReader(iis));
      }

      //read unencrypted, uncompressed,  data
      String dataLine;
      while ((dataLine = logLinesReader.readLine()) != null) {
        dataList.add(dataLine);
      }
      logLinesReader.close();
    } catch (IOException e) {
      throw new APIClientException("Failed reading file: " + fileName + "for partition:" + partition, e);
    }

    return dataList;

  }

  public static Map<String, String> parseHeader(String headerData) {

    if (StringUtils.isBlank(headerData)) {
      return Collections.emptyMap();
    }

    String[] hdrLines = headerData.split("\\r?\\n");
    if (hdrLines == null || hdrLines.length < 1) {
      return Collections.emptyMap();
    }

    Map<String, String> hdrMap = new HashMap<>(hdrLines.length);
    for (String hdrLine : hdrLines) {
      String[] kv = hdrLine.split(":");
      if (kv != null && kv.length > 1) {
        hdrMap.put(kv[0], kv[1]);
      }
    }
    return hdrMap;
  }

  private List<SourceRecord> createRecords(String topic, Map<String, Object> partition, Map<String, Object> offset, String fileName, List<Object> dataList) {

    List<SourceRecord> result = new ArrayList<>(dataList.size());
    for (Object value : dataList) {
      SourceRecord r = new SourceRecord(partition, offset, topic, null, null, null, value);
      r.headers().addString("incapsula.logFile", partition.get(PARTITION_ENDPOINT_KEY) + fileName);
      r.headers().addString("incapsula.appId", (String) partition.get(PARTITION_APP_ID_KEY));
      result.add(r);
    }

    return result;
  }

  private List getIndexFile(Map<String, Object> partition) throws APIClientException {
    if (indexFiles.containsKey(partition)) {
      return indexFiles.get(partition);
    }

    List<String> newFile = new ArrayList<>();
    //fetch indexfile from remote
    Request.Builder requestBuilder = new Request.Builder()
        .url(partition.get(PARTITION_ENDPOINT_KEY) + "/logs.index")
        .get();
    OkHttpClient httpClient = httpClients.get(partition);
    Request request = requestBuilder.build();
    try (Response response = httpClient.newCall(request).execute()) { //TODO: implement async call

      if (!response.isSuccessful()) {
        log.error("Failed to get logs.index for: {}", partition.get(PARTITION_APP_ID_KEY));
        throw new APIClientException("Failed to get logs.index. Unexpected code: " + response);
      }

      BufferedReader in = new BufferedReader(new InputStreamReader(response.body().byteStream()));
      String fileName;
      while ((fileName = in.readLine()) != null) {
        newFile.add(fileName);
      }
      in.close();

    } catch (IOException e) {
      throw new APIClientException("getIndexFile failed for: " + request.url(), e);
    }

    indexFiles.put(partition, newFile);

    return newFile;
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {
    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    return initialOffsets.get(partition);
  }

  @Override
  public void close() {

  }

}
