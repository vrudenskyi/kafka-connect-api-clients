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
package com.mckesson.kafka.connect.workday;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mckesson.kafka.connect.utils.OkHttpClientConfig;
import com.mckesson.kafka.connect.utils.UrlBuilder;

import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.FormBody;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.Route;

/**
 * Two-in-one interceptor and authenticator to implement preemptive auth
 */
public class WorkdayPreemptiveAuthenticator implements Authenticator, Configurable, Interceptor  {

  private static final Logger log = LoggerFactory.getLogger(WorkdayPreemptiveAuthenticator.class);

  private String tenantName;
  private String tenantUrl;
  private String username;
  private Password password;

  private Password refreshToken;
  private Password accessToken;
  private OkHttpClient httpClient;
  
  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig conf = new SimpleConfig(WorkdayAuditLogsClient.CONFIG_DEF, configs);
    this.tenantUrl = conf.getString(WorkdayAuditLogsClient.URL_CONFIG);
    this.tenantName = conf.getString(WorkdayAuditLogsClient.TENANT_NAME_CONFIG);
    this.refreshToken = conf.getPassword(WorkdayAuditLogsClient.REFRESH_TOKEN_CONFIG);
    this.username = conf.getString(WorkdayAuditLogsClient.USER_CONFIG);
    this.password = conf.getPassword(WorkdayAuditLogsClient.PASSWORD_CONFIG);

    try {
      this.httpClient = OkHttpClientConfig.buildClient(configs);
    } catch (Exception e) {
      throw new ConfigException("Failed to create httpClient for WorkdayPreemptiveAuthenticator", e);
    }
  }

  @Override
  public Request authenticate(Route route, Response resp) throws IOException {

    //update access tiken if new or expired  
    if (accessToken == null || resp.code() != 0) {
      log.debug("Authenticate WorkdayClient url:{}, tenant:{}", tenantUrl, tenantName);
      UrlBuilder urlB = new UrlBuilder(tenantUrl + WorkdayAuditLogsClient.TOKEN_ENDPOINT_TEMPLATE);
      urlB.routeParam("tenantName", tenantName);
      
      RequestBody formBody = new FormBody.Builder()
          .add("grant_type", "refresh_token")
          .add("refresh_token", refreshToken.value())
          .build();      

      String credential = Credentials.basic(this.username, this.password.value());
      Request.Builder requestBuilder = new Request.Builder()
          .url(urlB.getUrl())
          .header("Authorization", credential)
          .header("Accept", "*/*")
          .post(formBody);

      Request r = requestBuilder.build();

      try (Response authResponse = this.httpClient.newCall(r).execute()) {
        if (!authResponse.isSuccessful()) {
          log.error("Unexpected code: {}  \n\t with body: {}\n", authResponse, authResponse.body().string());
          throw new IOException("Unexpected code " + authResponse);
        }

        ObjectMapper mapper = new ObjectMapper();
        JsonNode respData = mapper.readValue(authResponse.body().byteStream(), JsonNode.class);
        JsonNode accessToken = respData.get("access_token");
        if (accessToken.isMissingNode()) {
          log.error("Missing 'access_token' in response: {}  \n\t with body: {}\n", authResponse, authResponse.body().string());
          throw new IOException("Unexpected code " + authResponse);

        }
        this.accessToken = new Password(accessToken.asText());
      }
    }

    return resp.request().newBuilder().header("Authorization", "Bearer " + accessToken.value()).build();
  }

  @Override
  public Response intercept(Chain chain) throws IOException {
    if (chain.request().header("Authorization") == null) {
      Response.Builder rb = new Response.Builder().request(chain.request())
          .protocol(Protocol.HTTP_1_0)
          .message("PreemptiveAuthInterceptor")
          .code(0);
      return chain.proceed(authenticate(null, rb.build()));
    }
    return chain.proceed(chain.request());
  }

}
