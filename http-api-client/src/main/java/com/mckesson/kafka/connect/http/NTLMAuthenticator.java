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
package com.mckesson.kafka.connect.http;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import okhttp3.Authenticator;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

/**
 * Ntlm authnticator
 * 
 * @author ew4ahmz
 *
 */
public class NTLMAuthenticator implements Authenticator, Configurable {

  public static final String DOMAIN_CONFIG = "http.auth.ntlm.domain";
  public static final String DOMAIN_DEFAULT = "";
  public static final String USER_CONFIG = "http.auth.ntlm.user";
  public static final String PASSWORD_CONFIG = "http.auth.ntlm.password";
  public static final String WORKSTATION_CONFIG = "http.auth.ntlm.workstation";
  public static final String WORKSTATION_DEFAULT = "kafka-connect";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(USER_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Ntml auth username. Required")
      .define(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Ntlm auth password. Required")
      .define(DOMAIN_CONFIG, ConfigDef.Type.STRING, DOMAIN_DEFAULT, ConfigDef.Importance.MEDIUM, "Domain.")
      .define(WORKSTATION_CONFIG, ConfigDef.Type.STRING, WORKSTATION_DEFAULT, ConfigDef.Importance.LOW, "Workstation");

  final NTLMEngineImpl engine = new NTLMEngineImpl();
  private String domain;
  private String username;
  private Password password;
  private String ntlmMsg1;
  private String workstation;

  @Override
  public void configure(Map<String, ?> configs) {

    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.username = conf.getString(USER_CONFIG);
    this.password = conf.getPassword(PASSWORD_CONFIG);
    this.domain = conf.getString(DOMAIN_CONFIG);
    this.workstation = conf.getString(WORKSTATION_CONFIG);
    this.ntlmMsg1 = engine.generateType1Msg(null, null);
  }

  @Override
  public Request authenticate(Route route, Response response) throws IOException {
    final List<String> wwwAuthenticate = response.headers().values("WWW-Authenticate");
    if (wwwAuthenticate.contains("NTLM")) {
      return response.request().newBuilder().header("Authorization", "NTLM " + ntlmMsg1).build();
    }
    String ntlmMsg3 = null;
    try {
      ntlmMsg3 = engine.generateType3Msg(username, password.value(), domain, workstation, wwwAuthenticate.get(0).substring(5));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return response.request().newBuilder().header("Authorization", "NTLM " + ntlmMsg3).build();
  }

}
