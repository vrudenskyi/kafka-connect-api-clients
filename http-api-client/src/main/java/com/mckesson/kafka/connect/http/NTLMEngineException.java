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

public class NTLMEngineException extends RuntimeException {

  public NTLMEngineException() {
    super();
  }

  /**
   * Creates a new NTLMEngineException with the specified message.
   *
   * @param message the exception detail message
   */
  public NTLMEngineException(final String message) {
    super(message);
  }

  /**
   * Creates a new NTLMEngineException with the specified detail message and cause.
   *
   * @param message the exception detail message
   * @param cause the {@code Throwable} that caused this exception, or {@code null}
   * if the cause is unavailable, unknown, or not a {@code Throwable}
   */
  public NTLMEngineException(final String message, final Throwable cause) {
    super(message, cause);
  }
}
