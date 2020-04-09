/*
 * Copyright 2015-2019 the original author or authors.
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

package com.jauntsdn.rsocket.lease;

import com.jauntsdn.rsocket.Availability;
import com.jauntsdn.rsocket.frame.LeaseFrameFlyweight;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public interface RequesterLeaseHandler extends Availability {

  void requestStarted();

  void receive(ByteBuf leaseFrame);

  final class Impl implements RequesterLeaseHandler {
    private volatile LeaseImpl currentLease = LeaseImpl.empty();

    @Override
    public void requestStarted() {
      currentLease.use();
    }

    @Override
    public void receive(ByteBuf leaseFrame) {
      int numberOfRequests = LeaseFrameFlyweight.numRequests(leaseFrame);
      int timeToLiveMillis = LeaseFrameFlyweight.ttl(leaseFrame);
      LeaseImpl lease = LeaseImpl.create(timeToLiveMillis, numberOfRequests, Unpooled.EMPTY_BUFFER);
      currentLease = lease;
    }

    @Override
    public double availability() {
      return currentLease.availability();
    }
  }
}
