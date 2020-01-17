/*
 * Copyright 2015-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jauntsdn.rsocket.exceptions;

final class ConnectionCloseExceptionTest implements RSocketExceptionTest<ConnectionCloseException> {

  @Override
  public ConnectionCloseException getException(String message) {
    return new ConnectionCloseException(message);
  }

  @Override
  public ConnectionCloseException getException(String message, Throwable cause) {
    return new ConnectionCloseException(message, cause);
  }

  @Override
  public int getSpecifiedErrorCode() {
    return 0x00000102;
  }
}
