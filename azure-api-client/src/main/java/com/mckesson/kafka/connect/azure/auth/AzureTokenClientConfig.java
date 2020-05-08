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
package com.mckesson.kafka.connect.azure.auth;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.types.Password;

public class AzureTokenClientConfig extends AbstractConfig {

  public static final String CLIENT_ID_CONFIG = "azure.auth.clientId";
  public static final String CLIENT_SECRET_CONFIG = "azure.auth.clientSecret";
  public static final Password CLIENT_SECRET_DEFAULT = new Password("");

  public static final String SUBSCRIPTION_ID_CONFIG = "azure.subscription.id";
  public static final String TENANT_ID_CONFIG = "azure.tenant.id";
  public static final String RESOURCE_CONFIG = "azure.auth.resource";

  public static final String USER_CONFIG = "azure.auth.user";
  public static final String PASSWORD_CONFIG = "azure.auth.password";
  public static final Password PASSWORD_DEFAULT = new Password("");

  public static final String KEYSTORE_TYPE_CONFIG = "azure.auth.keystore.type";
  public static final String KEYSTORE_TYPE_DEFAULT = "JKS";

  public static final String KEYSTORE_PATH_CONFIG = "azure.auth.keystore.path";
  public static final String KEYSTORE_PASSWORD_CONFIG = "azure.auth.keystore.password";
  public static final Password KEYSTORE_PASSWORD_DEFAULT = new Password("");

  public static final String KEY_ALIAS_CONFIG = "azure.auth.key.alias";
  public static final String KEY_PASSWORD_CONFIG = "azure.auth.key.password";
  public static final Password KEY_PASSWORD_DEFAULT = new Password("");

  public static final String TOKEN_RENEW_WINDOW_CONFIG = "azure.auth.token.renew.window";
  public static final Long TOKEN_RENEW_WINDOW_DEFAULT = 60L * 1000; //60 seconds
  public static final String TOKEN_LIFETIME_CONFIG = "azure.auth.tiken.lifetime";
  public static final Long TOKEN_LIFETIME_DEFAULT = 60L * 50 * 1000; // 1 minute

  protected static ConfigDef baseConfigDef() {
    final ConfigDef configDef = new ConfigDef();
    addConfig(configDef);
    return configDef;
  }

  public static void addConfig(ConfigDef config) {

    config
        .define(CLIENT_ID_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Azure Client Id")
        .define(CLIENT_SECRET_CONFIG, Type.PASSWORD, CLIENT_SECRET_DEFAULT, Importance.HIGH, "Azure Client secret")
        .define(SUBSCRIPTION_ID_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Azure Subscription Id")
        .define(TENANT_ID_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Azure tenant Id")
        .define(RESOURCE_CONFIG, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH, "Azure auth resource")

        .define(USER_CONFIG, Type.STRING, null, Importance.MEDIUM, "Username for user/password based login")
        .define(PASSWORD_CONFIG, Type.PASSWORD, PASSWORD_DEFAULT, Importance.MEDIUM, "Password for user/password based login")

        .define(KEYSTORE_TYPE_CONFIG, Type.STRING, KEYSTORE_TYPE_DEFAULT, Importance.MEDIUM, "Keystore type")
        .define(KEYSTORE_PATH_CONFIG, Type.STRING, KEYSTORE_TYPE_DEFAULT, Importance.MEDIUM, "Keystore path")
        .define(KEYSTORE_PASSWORD_CONFIG, Type.PASSWORD, KEYSTORE_PASSWORD_DEFAULT, Importance.MEDIUM, "Keystore password")

        .define(KEY_ALIAS_CONFIG, Type.STRING, null, Importance.MEDIUM, "Key alias")
        .define(KEY_PASSWORD_CONFIG, Type.PASSWORD, KEY_PASSWORD_DEFAULT, Importance.MEDIUM, "Key password")

        .define(TOKEN_RENEW_WINDOW_CONFIG, Type.LONG, TOKEN_RENEW_WINDOW_DEFAULT, Importance.LOW, "Renew token window. Renew will start before expiration. default: 1 minute")
        .define(TOKEN_LIFETIME_CONFIG, Type.LONG, TOKEN_LIFETIME_DEFAULT, Importance.LOW, "Token lifetime. default: 1 hour");

  }

  public static final ConfigDef CONFIG = baseConfigDef();

  public AzureTokenClientConfig(Map<String, ?> props) {
    super(CONFIG, props);
  }

  public AzureTokenClientConfig(ConfigDef configDef, Map<String, String> props) {
    super(configDef, props);
  }

}
