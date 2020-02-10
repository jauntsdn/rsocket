package com.jauntsdn.rsocket.frame.decoder;

import com.jauntsdn.rsocket.Payload;
import com.jauntsdn.rsocket.frame.*;
import com.jauntsdn.rsocket.util.DefaultPayload;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;

/** Default Frame decoder that copies content into new non-pooled heap buffer. */
class DefaultPayloadDecoder implements PayloadDecoder {
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  @Override
  public Payload apply(ByteBuf byteBuf) {
    ByteBuf m;
    ByteBuf d;
    FrameType type = FrameHeaderFlyweight.frameType(byteBuf);
    switch (type) {
      case REQUEST_FNF:
        d = RequestFireAndForgetFrameFlyweight.data(byteBuf);
        m = RequestFireAndForgetFrameFlyweight.metadata(byteBuf);
        break;
      case REQUEST_RESPONSE:
        d = RequestResponseFrameFlyweight.data(byteBuf);
        m = RequestResponseFrameFlyweight.metadata(byteBuf);
        break;
      case REQUEST_STREAM:
        d = RequestStreamFrameFlyweight.data(byteBuf);
        m = RequestStreamFrameFlyweight.metadata(byteBuf);
        break;
      case REQUEST_CHANNEL:
        d = RequestChannelFrameFlyweight.data(byteBuf);
        m = RequestChannelFrameFlyweight.metadata(byteBuf);
        break;
      case NEXT:
      case NEXT_COMPLETE:
        d = PayloadFrameFlyweight.data(byteBuf);
        m = PayloadFrameFlyweight.metadata(byteBuf);
        break;
      case METADATA_PUSH:
        d = Unpooled.EMPTY_BUFFER;
        m = MetadataPushFrameFlyweight.metadata(byteBuf);
        break;
      default:
        throw new IllegalArgumentException("unsupported frame type: " + type);
    }

    return DefaultPayload.create(copy(d), copy(m));
  }

  private ByteBuffer copy(ByteBuf byteBuf) {
    if (byteBuf == null) {
      return null;
    }
    int readableBytes = byteBuf.readableBytes();
    if (readableBytes == 0) {
      return EMPTY_BUFFER;
    }
    ByteBuffer b = ByteBuffer.allocate(readableBytes);
    b.put(byteBuf.nioBuffer());
    b.flip();
    return b;
  }
}
