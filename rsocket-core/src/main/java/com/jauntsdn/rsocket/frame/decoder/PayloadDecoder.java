package com.jauntsdn.rsocket.frame.decoder;

import com.jauntsdn.rsocket.Payload;
import io.netty.buffer.ByteBuf;
import java.util.function.Function;

public interface PayloadDecoder extends Function<ByteBuf, Payload> {
  PayloadDecoder DEFAULT = new DefaultPayloadDecoder();
  PayloadDecoder ZERO_COPY = new ZeroCopyPayloadDecoder();
}
