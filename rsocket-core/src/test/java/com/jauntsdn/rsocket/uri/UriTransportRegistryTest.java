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

package com.jauntsdn.rsocket.uri;

import static org.junit.Assert.assertTrue;

import com.jauntsdn.rsocket.DuplexConnection;
import com.jauntsdn.rsocket.frame.FrameLengthFlyweight;
import com.jauntsdn.rsocket.test.util.TestDuplexConnection;
import com.jauntsdn.rsocket.transport.ClientTransport;
import org.junit.Test;

public class UriTransportRegistryTest {
  private static final int maxFrameSize = FrameLengthFlyweight.FRAME_LENGTH_MASK;

  @Test
  public void testTestRegistered() {
    ClientTransport test = UriTransportRegistry.clientForUri("test://test");

    DuplexConnection duplexConnection = test.connect(maxFrameSize).block();

    assertTrue(duplexConnection instanceof TestDuplexConnection);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testTestUnregistered() {
    ClientTransport test = UriTransportRegistry.clientForUri("mailto://bonson@baulsupp.net");

    test.connect(maxFrameSize).block();
  }
}
