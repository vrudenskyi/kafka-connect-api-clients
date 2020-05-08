package com.mckesson.kafka.connect.okta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Link;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.mckesson.kafka.connect.http.HttpAPIClient;
import com.mckesson.kafka.connect.source.APIClientException;
import com.mckesson.kafka.connect.source.PollableAPIClient;
import com.mckesson.kafka.connect.utils.UrlBuilder;

import okhttp3.Request;
import okhttp3.Request.Builder;
import okhttp3.Response;

/**
 * 
 * https://developer.okta.com/docs/reference/api/system-log/#list-events 
 * https://developer.okta.com/docs/reference/api-overview/#authentication
 */
public class OktaSystemLogAPIClient extends HttpAPIClient {

  private static final Logger log = LoggerFactory.getLogger(OktaSystemLogAPIClient.class);

  public static final String OKTA_URI_CONFIG = "okta.serverUri";
  public static final String OKTA_TOKEN_CONFIG = "okta.token";

  public static final String OKTA_ENDPOINT_CONFIG = "okta.endpoint";
  public static final String OKTA_ENDPOINT_DEFAULT = "/api/v1/logs?since={since}&sortOrder={sortOrder}&limit={limit}";

  public static final String LIMIT_CONFIG = "okta.limit";
  public static final int LIMIT_DEFAULT = 1000;

  public static final String SORT_ORDER_CONFIG = "okta.sortOrder";
  public static final String SORT_ORDER_DEFAULT = "ASCENDING";

  public static final String INITIAL_OFFSET_DEFAULT = "2000-01-01T00:00:00.000Z";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(OKTA_URI_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Okta host. Required")
      .define(OKTA_TOKEN_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Okta token. Required")
      .define(OKTA_ENDPOINT_CONFIG, ConfigDef.Type.STRING, OKTA_ENDPOINT_DEFAULT, ConfigDef.Importance.HIGH, "Okta endpoint")
      .define(LIMIT_CONFIG, ConfigDef.Type.INT, LIMIT_DEFAULT, ConfigDef.Importance.HIGH, "Okta limit. default: 1000")
      .define(SORT_ORDER_CONFIG, ConfigDef.Type.STRING, SORT_ORDER_DEFAULT, ConfigDef.Importance.HIGH, "Sort order")
      .define(PollableAPIClient.INITIAL_OFFSET_CONFIG, ConfigDef.Type.STRING, INITIAL_OFFSET_DEFAULT, ConfigDef.Importance.HIGH, "Initial offset");

  public static final String OFFSET_NEXT_KEY = "______next";

  private String initialOffset;
  private int limit;
  private String sortOrder;
  private Password token;

  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.initialOffset = conf.getString(PollableAPIClient.INITIAL_OFFSET_CONFIG);
    this.token = conf.getPassword(OKTA_TOKEN_CONFIG);
    this.limit = conf.getInt(LIMIT_CONFIG);
    this.sortOrder = conf.getString(SORT_ORDER_CONFIG);
    String oktaUri = conf.getString(OKTA_URI_CONFIG);
    String oktaEndpoint = conf.getString(OKTA_ENDPOINT_CONFIG);
    ((Map<String, Object>) configs).put(HttpAPIClient.SERVER_URI_CONFIG, oktaUri);
    ((Map<String, Object>) configs).put(HttpAPIClient.ENDPPOINT_CONFIG, oktaEndpoint);
    super.configure(configs);
  }

  @Override
  protected Builder getRequestBuilder(Map<String, Object> partition, Map<String, Object> offset, int itemsToPoll) {
    Builder requestBuilder = new Request.Builder()
        .get()
        .url((String) offset.get(OFFSET_NEXT_KEY))
        .addHeader("Accept", "application/json")
        .addHeader("Content-Type", "application/json")
        .addHeader("Authorization", "SSWS " + token.value());
    return requestBuilder;
  }

  @Override
  public List<Object> extractData(Map<String, Object> partition, Map<String, Object> offset, Response response) throws APIClientException {

    List<Object> data = Collections.emptyList();
    JsonNode node;
    try {
      node = mapper.readValue(response.body().byteStream(), JsonNode.class);
    } catch (Exception e1) {
      throw new APIClientException("failed to read response", e1);
    } finally {
      response.close();
    }

    String dataPointer = "";
    JsonNode dataNode = node.at(dataPointer);
    if (dataNode.isMissingNode()) {
      log.warn("'{}'node is missing in response. Whole response to be returned", dataPointer);
      try {
        data = Arrays.asList(mapper.writeValueAsString(node));
      } catch (JsonProcessingException e) {
        data = Arrays.asList(node);
      }

    } else if (dataNode.isArray()) {
      log.debug("Extracting data from array node '{}' (size:{})", dataPointer, dataNode.size());
      data = new ArrayList<>(dataNode.size());
      for (JsonNode rec : dataNode) {
        try {
          data.add(mapper.writeValueAsString(rec));
        } catch (JsonProcessingException e) {
          data.add(rec);
        }
      }
    } else {
      log.warn("{} node is not an array, returned as singe object", dataPointer);
      try {
        data = Arrays.asList(mapper.writeValueAsString(dataNode));
      } catch (JsonProcessingException e) {
        data = Arrays.asList(dataNode);
      }
    }

    return data;
  }

  @Override
  protected void updateOffset(String topic, Map<String, Object> partition, Map<String, Object> offset, Response response, List<SourceRecord> pollResult) {
    List<String> linkHdrs = response.headers("Link");
    String currentOfffset = (String) offset.get(OFFSET_NEXT_KEY);
    for (String linkHdr : linkHdrs) {
      Link link = Link.valueOf(linkHdr);
      String rel = link.getRel();
      if ("next".equals(rel)) {
        String newOffset = link.getUri().toString();
        if (!currentOfffset.equals(newOffset)) {
          offset.put(OFFSET_NEXT_KEY, newOffset);
          log.debug("Updated offset for partition: {} \n new offset: {}", partition, offset);
        }
        //'next' link found exit ignoring the others
        break;
      }
    }
  }

  @Override
  public Map<String, Object> initialOffset(Map<String, Object> partition) throws APIClientException {
    Map<String, Object> offset = new HashMap<>(1);
    log.debug("Built initial 'next' url for paartition: {}, next: {}", partition, this.initialOffset);

    UrlBuilder ub = new UrlBuilder(partition.get(PARTITION_URL_KEY).toString())
        .routeParam("since", this.initialOffset)
        .routeParam("limit", String.valueOf(this.limit))
        .routeParam("sortOrder", this.sortOrder);
    offset.put(OFFSET_NEXT_KEY, ub.getUrl());
    log.debug("For partition: {} Initial Offset configured: {}", partition, offset);
    return offset;
  }

}
