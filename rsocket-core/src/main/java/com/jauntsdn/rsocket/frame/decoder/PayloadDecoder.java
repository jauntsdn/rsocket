package com.jauntsdn.rsocket.frame.decoder;

import com.jauntsdn.rsocket.Payload;
import com.jauntsdn.rsocket.frame.FrameType;
import io.netty.buffer.ByteBuf;
import java.util.function.BiFunction;

public interface PayloadDecoder extends BiFunction<ByteBuf, FrameType, Payload> {
  PayloadDecoder DEFAULT = new DefaultPayloadDecoder();
  PayloadDecoder ZERO_COPY = new ZeroCopyPayloadDecoder();
}
