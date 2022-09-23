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
package com.vrudenskyi.kafka.connect.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.vrudensk.kafka.connect.utils.OkHttpClientConfig;
import com.vrudensk.kafka.connect.utils.UrlBuilder;
import com.vrudenskyi.kafka.connect.source.APIClientException;
import com.vrudenskyi.kafka.connect.source.PollableAPIClient;

import okhttp3.Authenticator;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.internal.http.HttpMethod;

/**
 * Generic implementation for simple http client based pollers. 
 * 
 * @author Vitalii Rudenskyi
 *
 */
public abstract class HttpAPIClient implements PollableAPIClient {

  private static final Logger log = LoggerFactory.getLogger(HttpAPIClient.class);

  public static final String SERVER_URI_CONFIG = "http.serverUri";
  public static final String ENDPPOINT_CONFIG = "http.endpoint";
  public static final String METHOD_CONFIG = "http.method";
  public static final String METHOD_DEFAULT = "GET";

  public static final String AUTH_TYPE_CONFIG = "http.auth.type";
  public static final String AUTH_TYPE_DEFAULT = "none";
  public static final String AUTH_CLASS_CONFIG = "http.auth.class";

  public static final String REQUEST_PAYLOAD_CONFIG = "http.request.payload";
  public static final String REQUEST_PAYLOAD_DEFAULT = "";

  public static final String REQUEST_CONTENT_TYPE_CONFIG = "http.request.contentType";
  public static final String REQUEST_CONTENT_TYPE_DEFAULT = "text/plain";

  public static final String REQUEST_HEADERS_CONFIG = "http.request.headers";
  public static final String HEADER_NAME_CONFIG = "name";
  public static final String HEADER_VALUE_CONFIG = "value";

  protected enum AuthType {
    NONE, BASIC, NTLM, CUSTOM;

    public Authenticator getAuthenticator() {
      Authenticator auth;
      switch (this) {
        case BASIC:
          auth = new BasicAuthenticator();
          break;

        case NTLM:
          auth = new NTLMAuthenticator();
          break;

        case NONE:
          auth = new NoneAuthenticator();
          break;

        case CUSTOM:
        default:
          auth = null;
          break;
      }

      return auth;

    }

  };

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(SERVER_URI_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Server URI. Required.")
      .define(ENDPPOINT_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Endpoint. Required")
      .define(METHOD_CONFIG, ConfigDef.Type.STRING, METHOD_DEFAULT, ConfigDef.Importance.MEDIUM, "Http method. default: GET")
      .define(AUTH_TYPE_CONFIG, ConfigDef.Type.STRING, AUTH_TYPE_DEFAULT, ConfigDef.Importance.MEDIUM, "Authtype. Default: none")
      .define(REQUEST_PAYLOAD_CONFIG, ConfigDef.Type.STRING, REQUEST_PAYLOAD_DEFAULT, ConfigDef.Importance.MEDIUM, "Http request payload. Default: <empty-string>")
      .define(REQUEST_CONTENT_TYPE_CONFIG, ConfigDef.Type.STRING, REQUEST_CONTENT_TYPE_DEFAULT, ConfigDef.Importance.MEDIUM, "Request content type. default: " + REQUEST_CONTENT_TYPE_DEFAULT)
      .define(REQUEST_HEADERS_CONFIG, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM, "Http request headers")
      .define(AUTH_CLASS_CONFIG, ConfigDef.Type.CLASS, null, ConfigDef.Importance.LOW, "Custom auth class");

  public static final ConfigDef HEADER_DEF = new ConfigDef()
      .define(HEADER_NAME_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Header name")
      .define(HEADER_VALUE_CONFIG, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, "Header value");

  protected static final String PARTITION_URL_KEY = "___URL";
  protected static final String PARTITION_METHOD_KEY = "___METHOD";

  protected final ObjectMapper mapper = new ObjectMapper();
  protected OkHttpClient httpClient;
  protected Map<String, List<String>> httpHeaders;

  protected String serverUri;
  protected String endpoint;
  protected String httpMethod;
  protected String requestPayload;
  protected String requestContentType;

  @Override
  public void configure(Map<String, ?> configs) {

    log.debug("Configuring HttpAPIClient...");
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.serverUri = conf.getString(SERVER_URI_CONFIG);
    this.endpoint = conf.getString(ENDPPOINT_CONFIG);
    this.httpMethod = conf.getString(METHOD_CONFIG);
    this.requestPayload = conf.getString(REQUEST_PAYLOAD_CONFIG);
    this.requestContentType = conf.getString(REQUEST_CONTENT_TYPE_CONFIG);

    try {
      this.httpClient = httpClientBuilder(configs).build();
    } catch (Exception e) {
      throw new ConfigException("Failed to configure http client", e);
    }

    List<String> headersList = conf.getList(REQUEST_HEADERS_CONFIG);
    if (headersList != null && headersList.size() > 0) {
      httpHeaders = new LinkedHashMap<>();
      for (String alias : headersList) {
        SimpleConfig hc = new SimpleConfig(HEADER_DEF, conf.originalsWithPrefix(REQUEST_HEADERS_CONFIG + "." + alias + "."));
        String name = StringUtils.isBlank(hc.getString(HEADER_NAME_CONFIG)) ? alias : hc.getString(HEADER_NAME_CONFIG);
        String value = hc.getString(HEADER_VALUE_CONFIG);
        httpHeaders.putIfAbsent(name, new ArrayList<>());
        httpHeaders.get(name).add(value);
      }
    }

    log.debug("HttpAPIClient configured.");
  }

  protected OkHttpClient.Builder httpClientBuilder(Map<String, ?> configs) throws Exception {

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);

    Authenticator authenticator;
    AuthType authType = AuthType.valueOf(conf.getString(AUTH_TYPE_CONFIG).toUpperCase());
    if (AuthType.CUSTOM.equals(authType)) {
      authenticator = conf.getConfiguredInstance(AUTH_CLASS_CONFIG, Authenticator.class);
    } else {
      authenticator = authType.getAuthenticator();
      ((Configurable) authenticator).configure(configs);
    }

    return OkHttpClientConfig.builder(configs, authenticator);
  }

  @Override
  public List<SourceRecord> poll(String topic, Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, AtomicBoolean stop) throws APIClientException {

    List<SourceRecord> pollResult;
    Request.Builder rBuilder = getRequestBuilder(partition, offset, itemsToPoll);
    if (rBuilder == null) {
      log.debug("No request built, exit poll");
      return Collections.emptyList();
    }

    Request request = rBuilder.build();
    try (Response response = httpClient.newCall(request).execute()) { //TODO: implement async call
      List<Object> data = processResponse(partition, offset, response);
      pollResult = createRecords(topic, partition, offset, data);
      updateOffset(topic, partition, offset, response, pollResult);
    } catch (IOException e) {
      throw new APIClientException(e);
    }
    return pollResult;
  }

  /**
   * When data extracted and records created update offset.
   * 
   * @param topic
   * @param partition
   * @param offset
   * @param response 
   * @param pollResult
   */
  protected void updateOffset(String topic, Map<String, Object> partition, Map<String, Object> offset, Response response, List<SourceRecord> pollResult) {

  }

  /**
   * Checks response from server and calls {@code extractData} method
   * 
   * @param partition
   * @param offset
   * @param response
   * @return
   * @throws APIClientException
   * @throws IOException
   */
  protected List<Object> processResponse(Map<String, Object> partition, Map<String, Object> offset, Response response) throws APIClientException, IOException {

    if (!response.isSuccessful()) {
      log.error("Unexpected code: {}  \n\t with body: {} \n\t for partition: {} \n\t offset: {}\n", response, response.body().string(), partition, offset);
      throw new APIClientException("Unexpected code: " + response);
    }
    log.debug("HttpRequest:{} sucessfully finished. with code: {}", response.request().url(), response.code());
    return extractData(partition, offset, response);
  }

  public abstract List<Object> extractData(Map<String, Object> partition, Map<String, Object> offset, Response response) throws APIClientException;

  /**
   * Creates builder for the request
   * 
   * @param partition
   * @param offset
   * @param itemsToPoll
   * @return - can return null to skip the poll 
   */
  protected Request.Builder getRequestBuilder(Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll) {
    return getRequestBuilder(partition, offset, itemsToPoll, null, null);
  }

  /**
   * Creates builder for the request
   * 
   * @param partition
   * @param offset
   * @param itemsToPoll
   * @param urlParams
   * @param queryParams
   * @return - can return null to skip the poll 
   */
  protected Request.Builder getRequestBuilder(Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll, Map<String, String> urlParams, Map<String, String> queryParams) {

    UrlBuilder ub = new UrlBuilder(partition.get(PARTITION_URL_KEY).toString());

    if (MapUtils.isNotEmpty(urlParams)) {
      urlParams.forEach((name, value) -> {
        ub.routeParam(name, value);
      });
    }
    if (MapUtils.isNotEmpty(queryParams)) {
      queryParams.forEach((name, value) -> {
        ub.queryString(name, value);
      });
    }

    String method = partition.get(PARTITION_METHOD_KEY).toString();
    RequestBody body = null;
    if (HttpMethod.requiresRequestBody(method)) {
      body = RequestBody.create(MediaType.parse(this.requestContentType), this.requestPayload);
    }

    Request.Builder requestBuilder = new Request.Builder()
        .url(ub.getUrl())
        .method(method, body);

    if (httpHeaders != null && httpHeaders.size() > 0) {
      for (Map.Entry<String, List<String>> e : httpHeaders.entrySet()) {
        for (String value : e.getValue()) {
          requestBuilder.addHeader(e.getKey(), value);
        }
      }
    }

    return requestBuilder;
  }

  protected List<SourceRecord> createRecords(String topic, Map<String, Object> partition, Map<String, Object> offset, List<Object> dataList) {

    List<SourceRecord> result = new ArrayList<>(dataList.size());
    for (Object value : dataList) {
      SourceRecord r = new SourceRecord(partition, offset, topic, null, null, null, value);
      r.headers().addString("http.source", partition.get(PARTITION_URL_KEY).toString());
      result.add(r);
    }

    return result;
  }

  @Override
  public List<Map<String, Object>> partitions() throws APIClientException {
    List<Map<String, Object>> partitions = new ArrayList<>(1);
    Map<String, String> p = new HashMap<>(2);
    p.put(PARTITION_URL_KEY, new UrlBuilder(this.serverUri + this.endpoint).getUrl());
    p.put(PARTITION_METHOD_KEY, this.httpMethod);
    partitions.add(Collections.unmodifiableMap(p));
    return partitions;
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    Map<String, Object> offset = new HashMap<>(0);
    return offset;
  }

  @Override
  public void close() {
  }

}
