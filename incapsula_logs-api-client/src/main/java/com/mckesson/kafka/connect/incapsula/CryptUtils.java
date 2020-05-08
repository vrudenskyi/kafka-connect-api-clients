/**
 * Copyright  Vitalii Rudenskyi (vitalii.rudenskyi@gmail.com)
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

import java.security.KeyStore;
import java.security.PrivateKey;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class CryptUtils {
  public static byte[] aesDecrypt(byte[] data, byte[] secret, String cipherString, byte[] ivData) throws Exception {

    SecretKeySpec key = new SecretKeySpec(secret, "AES");
    Cipher cipher = Cipher.getInstance(cipherString);
    IvParameterSpec iv = new IvParameterSpec(ivData);
    cipher.init(Cipher.DECRYPT_MODE, key, iv);
    return cipher.doFinal(data);
  }

  public static byte[] rsaDecrypt(byte[] data, KeyStore keyStore, String keyAlias, char[] keyPass, String cipherString) throws Exception {
    PrivateKey privateKey = (PrivateKey) keyStore.getKey(keyAlias, keyPass);
    Cipher cipher = Cipher.getInstance(cipherString);
    cipher.init(Cipher.DECRYPT_MODE, privateKey);
    return cipher.doFinal(data);
  }
}
