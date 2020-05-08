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
import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import okhttp3.Authenticator;
import okhttp3.Credentials;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

/**
 * Basic authentificator implementation for HttpClient
 * 
 * @author Vitalii Rudenskyi
 *
 */
public class BasicAuthenticator implements Authenticator, Configurable {
  
  
  public static final String USER_CONFIG = "http.auth.basic.user";
  public static final String PASSWORD_CONFIG = "http.auth.basic.password";
  
  
  public static final ConfigDef CONFIG_DEF = new ConfigDef()
      .define(USER_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Basic auth username. Required")
      .define(PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Basic auth password. Required");


  private String username;
  private Password password;
  
  public BasicAuthenticator() { }
  public BasicAuthenticator(String username, Password password) {
    this.username = username;
    this.password = password;
  }

  
  
  @Override
  public void configure(Map<String, ?> configs) {
    SimpleConfig conf = new SimpleConfig(CONFIG_DEF, configs);
    this.username =  conf.getString(USER_CONFIG);
    this.password = conf.getPassword(PASSWORD_CONFIG);
  }

  @Override
  public Request authenticate(Route route, Response response) throws IOException {
    String credential = Credentials.basic(this.username, this.password.value());
    return response.request().newBuilder().header("Authorization", credential).build();
  }


}
