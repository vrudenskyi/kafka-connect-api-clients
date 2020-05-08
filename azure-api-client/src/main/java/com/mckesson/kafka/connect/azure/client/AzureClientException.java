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
package com.mckesson.kafka.connect.azure.client;

import com.mckesson.kafka.connect.source.APIClientException;

public class AzureClientException extends APIClientException {

  private int httpStatus;

  public AzureClientException(String s) {
    super(s);
  }
  
  public AzureClientException(String s, int httpStatus) {
    super(s);
    this.httpStatus = httpStatus;
  }
  

  public AzureClientException(String s, Throwable throwable) {
    super(s, throwable);
  }

  public AzureClientException(String s, int httpStatus, Throwable throwable) {
    super(s, throwable);
    this.httpStatus = httpStatus;
  }

  public AzureClientException(Throwable throwable) {
    super(throwable);
  }
  
  
  public int getHttpStatus() {
    return httpStatus;
  }

  

}
