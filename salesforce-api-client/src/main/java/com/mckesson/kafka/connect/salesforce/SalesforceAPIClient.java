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
package com.mckesson.kafka.connect.salesforce;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.force.api.ApiConfig;
import com.force.api.ApiVersion;
import com.force.api.ForceApi;
import com.force.api.QueryResult;
import com.mckesson.kafka.connect.source.PollableAPIClient;
import com.mckesson.kafka.connect.source.PollableSourceConfig;

public abstract class SalesforceAPIClient implements PollableAPIClient {

  public static final String SFDC_USER_CONFIG = "sfdc.user";
  public static final String SFDC_PASSWORD_CONFIG = "sfdc.password";
  public static final String SFDC_TOKEN_CONFIG = "sfdc.token";
  public static final String SFDC_URL_CONFIG = "sfdc.endpoint";
  public static final String SFDC_API_VERSION_CONFIG = "sfdc.version";
  public static final String SFDC_API_VERSION_DEFAULT = ApiVersion.DEFAULT_VERSION.name();
  public static final String SFDC_INITIAL_OFFSET_DEFAULT = "1971-01-01T00:00:00.000+0000";

  private static final ConfigDef CONFIG_DEF = PollableSourceConfig.addConfig(
      new ConfigDef()
          .define(SFDC_URL_CONFIG, Type.STRING, null, Importance.HIGH, "SFDC url")
          .define(SFDC_USER_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "SFDC  user")
          .define(SFDC_PASSWORD_CONFIG, Type.PASSWORD, null, Importance.HIGH, "SFDC password")
          .define(SFDC_TOKEN_CONFIG, Type.PASSWORD, null, Importance.HIGH, "SFDC token")
          .define(SFDC_API_VERSION_CONFIG, Type.STRING, SFDC_API_VERSION_DEFAULT, Importance.LOW, "SFDC API Version")
          .define(PollableAPIClient.INITIAL_OFFSET_CONFIG, Type.STRING, SFDC_INITIAL_OFFSET_DEFAULT, Importance.LOW, "Default initial offset"));

  protected static final String OFFSET_NRU_KEY = "_nextRecordsUrl";
  protected final ObjectMapper jacksonObjectMapper = new ObjectMapper();

  protected ApiConfig apiConfig;
  protected ForceApi forceApi;
  protected String initialOffset;

  protected <T> List<T> executeQuery(String query, Map<String, Object> offset, Class<T> clazz) {
    List<T> result;
    QueryResult<T> qr;
    if (offset != null && offset.containsKey(OFFSET_NRU_KEY)) {
      qr = forceApi.queryMore(offset.get(OFFSET_NRU_KEY).toString(), clazz);
    } else {
      qr = forceApi.query(query, clazz);
    }

    //upd offset's NRU
    if (offset != null) {
      if (StringUtils.isNoneBlank(qr.getNextRecordsUrl())) {
        offset.put(OFFSET_NRU_KEY, qr.getNextRecordsUrl());
      } else {
        offset.remove(OFFSET_NRU_KEY);
      }
    }

    result = qr.getRecords();
    return result;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    // TODO Auto-generated method stub

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.initialOffset = conf.getString(PollableAPIClient.INITIAL_OFFSET_CONFIG);

    this.apiConfig = new ApiConfig()
        .setApiVersion(ApiVersion.valueOf(conf.getString(SalesforceAPIClient.SFDC_API_VERSION_CONFIG)))
        .setUsername(conf.getString(SalesforceAPIClient.SFDC_USER_CONFIG))
        .setPassword(conf.getPassword(SalesforceAPIClient.SFDC_PASSWORD_CONFIG).value() + conf.getPassword(SalesforceAPIClient.SFDC_TOKEN_CONFIG).value());

    if (!StringUtils.isBlank(conf.getString(SFDC_URL_CONFIG))) {
      this.apiConfig.setForceURL(conf.getString(SFDC_URL_CONFIG));
    }
    this.forceApi = new ForceApi(this.apiConfig);

  }

  @Override
  public void close() {

  }

}
