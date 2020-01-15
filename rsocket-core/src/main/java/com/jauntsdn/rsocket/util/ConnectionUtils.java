package com.jauntsdn.rsocket.util;

import com.jauntsdn.rsocket.frame.ErrorFrameFlyweight;
import com.jauntsdn.rsocket.internal.ClientServerInputMultiplexer;
import io.netty.buffer.ByteBufAllocator;
import reactor.core.publisher.Mono;

public class ConnectionUtils {

  public static Mono<Void> sendError(
      ByteBufAllocator allocator, ClientServerInputMultiplexer multiplexer, Exception exception) {
    return multiplexer
        .asSetupConnection()
        .sendOne(ErrorFrameFlyweight.encode(allocator, 0, exception))
        .onErrorResume(err -> Mono.empty());
  }
}
