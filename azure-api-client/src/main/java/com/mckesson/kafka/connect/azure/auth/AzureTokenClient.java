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

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.naming.ServiceUnavailableException;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.Configurable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.aad.adal4j.AsymmetricKeyCredential;
import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationException;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientAssertion;
import com.microsoft.aad.adal4j.ClientCredential;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSHeader.Builder;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.util.Base64;
import com.nimbusds.jose.util.Base64URL;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

public class AzureTokenClient implements Configurable {

  /**
   * CONFIG
   */

  private static final Logger log = LoggerFactory.getLogger(AzureTokenClient.class);

  public static final String AUTHORITY_BASE = "https://login.windows.net/";
  public static final String TOKEN_ENDPOINT = "/oauth2/token";

  private String resource;
  private String tenantId;
  private String clientId;
  private String clientSecret;
  private String userName;
  private char[] password;

  private String keystoreType = "PKCS12";
  private String keystorePath;
  private char[] keystorePassword = "".toCharArray();

  private String keyAlias;
  private char[] keyPassword = "".toCharArray();

  private long tokenRenewWindow = 60 * 1000; //1 minute
  private long tokenLifetime = 60 * 60 * 1000; //60 minutes

  private boolean initialized = false;

  private AuthenticationResult currentToken;

  public AzureTokenClient() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    AzureTokenClientConfig tcConf = new AzureTokenClientConfig(configs);

    this
        .clientId(tcConf.getString(AzureTokenClientConfig.CLIENT_ID_CONFIG))
        .clientSecret(tcConf.getPassword(AzureTokenClientConfig.CLIENT_SECRET_CONFIG).value())
        .resource(tcConf.getString(AzureTokenClientConfig.RESOURCE_CONFIG))
        .tenantId(tcConf.getString(AzureTokenClientConfig.TENANT_ID_CONFIG))
        .keystoreType(tcConf.getString(AzureTokenClientConfig.KEYSTORE_TYPE_CONFIG))
        .keystorePath(tcConf.getString(AzureTokenClientConfig.KEYSTORE_PATH_CONFIG))
        .keystorePassword(tcConf.getPassword(AzureTokenClientConfig.KEYSTORE_PASSWORD_CONFIG).value().toCharArray())
        .keyAlias(tcConf.getString(AzureTokenClientConfig.KEY_ALIAS_CONFIG))
        .keyPassword(tcConf.getPassword(AzureTokenClientConfig.KEY_PASSWORD_CONFIG).value().toCharArray())
        .tokenRenewWindow(tcConf.getLong(AzureTokenClientConfig.TOKEN_RENEW_WINDOW_CONFIG))
        .tokenLifetime(tcConf.getLong(AzureTokenClientConfig.TOKEN_LIFETIME_CONFIG))
        .build();

  }

  public static AzureTokenClient newInstance() {
    return new AzureTokenClient();
  }

  public static AzureTokenClient configuredInstance(Map<String, ?> configs) {
    AzureTokenClient tc = AzureTokenClient.newInstance();
    tc.configure(configs);
    return tc;

  }

  public AzureTokenClient build() {
    log.debug("build tokenClient for tenant:{}, client:{}, resource:{}, clientSecret: {}", tenantId, clientId, resource, StringUtils.isNotBlank(clientSecret));
    if (StringUtils.isBlank(tenantId)) {
      throw new IllegalStateException("tenantId should not be empty");
    }

    if (StringUtils.isBlank(clientId)) {
      throw new IllegalStateException("clientId should not be empty");
    }

    if (StringUtils.isBlank(resource)) {
      throw new IllegalStateException("resource should not be empty");
    }

    if (StringUtils.isBlank(clientSecret) && StringUtils.isBlank(keystorePath) && StringUtils.isBlank(userName)) {
      throw new IllegalStateException("clientSecret or keystore or userName(password) should not be empty");
    }
    initialized = true;
    return this;

  }

  public AzureTokenClient resource(String resource) {
    this.resource = resource;
    return this;
  }

  public AzureTokenClient userName(String userName) {
    this.userName = userName;
    return this;
  }

  public AzureTokenClient password(char[] password) {
    this.password = password;
    return this;
  }

  public AzureTokenClient tenantId(String tenantId) {
    this.tenantId = tenantId;
    return this;
  }

  public String getTenantId() {
    return tenantId;
  }

  public AzureTokenClient clientId(String clientId) {
    this.clientId = clientId;
    return this;
  }

  public AzureTokenClient clientSecret(String clientSecret) {
    this.clientSecret = clientSecret;
    return this;
  }

  public AzureTokenClient keystoreType(String keystoreType) {
    this.keystoreType = keystoreType;
    return this;
  }

  public AzureTokenClient keystorePath(String keystorePath) {
    this.keystorePath = keystorePath;
    return this;
  }

  public AzureTokenClient keystorePassword(char[] keystorePassword) {
    this.keystorePassword = keystorePassword;
    return this;
  }

  public AzureTokenClient keyAlias(String keyAlias) {
    this.keyAlias = keyAlias;
    return this;
  }

  public AzureTokenClient keyPassword(char[] keyPassword) {
    this.keyPassword = keyPassword;
    return this;
  }

  public AzureTokenClient tokenRenewWindow(long tokenRenewWindow) {
    this.tokenRenewWindow = tokenRenewWindow;
    return this;
  }

  public AzureTokenClient tokenLifetime(long tokenLifetime) {
    this.tokenLifetime = tokenLifetime;
    return this;
  }

  public AuthenticationResult createNewToken() throws Exception {

    if (!initialized) {
      throw new IllegalStateException("AzureTokenClient was not initialized, invoke build() to initilize");
    }

    if (clientSecret != null) {
      log.debug("Create new access token from client secret");
      return getAccessTokenFromClientSecret();
    } else if (userName != null) {
      log.debug("Create new access token from user credentials");
      return getAccessTokenFromCredentials();
    } else {
      log.debug("Create new access token from client assertion");
      return getAccessTokenFromClientAssertion();
    }
  }

  public void resetCurrentToken() throws Exception {
    this.currentToken = createNewToken();
  }

  public AuthenticationResult getCurrentToken() throws Exception {
    if (!initialized) {
      throw new IllegalStateException("AzureTokenClient was not initialized, invoke build() to initilize");
    }

    if (currentToken == null) {
      log.debug("AccessToken is not created yet. Creating...");
      this.currentToken = createNewToken();
      return this.currentToken;
    }

    if (isCurrentTokenValid()) {
      return this.currentToken;
    }
    log.debug("Current token is invalid, need to be re-created");
    if (StringUtils.isNoneBlank(currentToken.getRefreshToken())) {
      log.debug("RefeshToken exists, renew currentToken from refresh");
      this.currentToken = getAccessTokenFromRefresh(currentToken.getRefreshToken());
    } else {
      log.debug("RefeshToken is not available, create new token");
      this.currentToken = createNewToken();
    }

    return this.currentToken;
  }

  private boolean isCurrentTokenValid() {
    if (this.currentToken == null) {
      return false;
    }
    long expireIn = this.currentToken.getExpiresOnDate().getTime() - System.currentTimeMillis();
    boolean isValid = expireIn > tokenRenewWindow;
    return isValid;
  }

  private AuthenticationResult getAccessTokenFromRefresh(String refreshToken) throws Exception {
    AuthenticationContext context = null;
    AuthenticationResult result = null;
    ExecutorService service = null;
    try {
      service = Executors.newFixedThreadPool(1);
      context = new AuthenticationContext(AUTHORITY_BASE + tenantId, true, service);

      ClientCredential cCred = new ClientCredential(clientId, clientSecret);
      Future<AuthenticationResult> future = context.acquireTokenByRefreshToken(refreshToken, cCred, null);

      result = future.get();
      if (result == null) {
        throw new ServiceUnavailableException("authentication result was null");
      }
    } finally {
      service.shutdown();
    }

    return result;
  }

  private AuthenticationResult getAccessTokenFromClientSecret() throws Exception {
    AuthenticationContext context = null;
    AuthenticationResult result = null;
    ExecutorService service = null;
    try {
      service = Executors.newFixedThreadPool(1);
      context = new AuthenticationContext(AUTHORITY_BASE + tenantId, true, service);

      ClientCredential cCred = new ClientCredential(clientId, clientSecret);
      Future<AuthenticationResult> future = context.acquireToken(resource, cCred, null);

      result = future.get();
      if (result == null) {
        throw new ServiceUnavailableException("authentication result was null");
      }
    } finally {
      service.shutdown();
    }

    return result;
  }

  private AuthenticationResult getAccessTokenFromCredentials() throws Exception {
    AuthenticationContext context = null;
    AuthenticationResult result = null;
    ExecutorService service = null;
    try {
      service = Executors.newFixedThreadPool(1);
      context = new AuthenticationContext(AUTHORITY_BASE + tenantId, true, service);

      Future<AuthenticationResult> future = context.acquireToken(resource, clientId, userName, new String(password), null);

      result = future.get();
      if (result == null) {
        throw new ServiceUnavailableException("authentication result was null");
      }
    } finally {
      service.shutdown();
    }

    return result;
  }

  private AuthenticationResult getAccessTokenFromClientAssertion() throws Exception {
    AuthenticationContext context = null;
    AuthenticationResult result = null;
    ExecutorService service = null;
    try {

      final KeyStore keystore = KeyStore.getInstance(keystoreType);
      keystore.load(new FileInputStream(keystorePath), keystorePassword);
      String alias = keyAlias;
      if (alias == null) {
        alias = keystore.aliases().nextElement();
      }
      final PrivateKey key = (PrivateKey) keystore.getKey(alias, keyPassword);
      final X509Certificate cert = (X509Certificate) keystore.getCertificate(alias);
      final AsymmetricKeyCredential asymmetricKeyCredential = AsymmetricKeyCredential.create(clientId, key, cert);

      ClientAssertion ca = buildAssertion(asymmetricKeyCredential, AUTHORITY_BASE + tenantId + TOKEN_ENDPOINT);
      service = Executors.newFixedThreadPool(1);
      context = new AuthenticationContext(AUTHORITY_BASE + tenantId, true, service);
      Future<AuthenticationResult> future = context.acquireToken(resource, ca, null);
      result = future.get();
      if (result == null) {
        throw new ServiceUnavailableException("authentication result was null");
      }
    } finally {
      service.shutdown();
    }

    return result;

  }

  private ClientAssertion buildAssertion(final AsymmetricKeyCredential credential, final String jwtAudience) throws AuthenticationException {
    if (credential == null) {
      throw new IllegalArgumentException("credential is null");
    }

    final long time = System.currentTimeMillis();

    final JWTClaimsSet claimsSet = new JWTClaimsSet.Builder().audience(Collections.singletonList(jwtAudience)).issuer(credential.getClientId()).notBeforeTime(new Date(time))
        .expirationTime(new Date(time + tokenLifetime)).subject(credential.getClientId()).build();

    SignedJWT jwt;
    try {
      JWSHeader.Builder builder = new Builder(JWSAlgorithm.RS256);
      List<Base64> certs = new ArrayList<Base64>();
      certs.add(new Base64(credential.getPublicCertificate()));
      builder.x509CertChain(certs);
      builder.x509CertThumbprint(new Base64URL(credential.getPublicCertificateHash()));
      jwt = new SignedJWT(builder.build(), claimsSet);
      final RSASSASigner signer = new RSASSASigner(credential.getKey());

      jwt.sign(signer);
    } catch (final Exception e) {
      throw new AuthenticationException(e);
    }

    return new ClientAssertion(jwt.serialize());
  }

}
