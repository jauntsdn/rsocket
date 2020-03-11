package com.jauntsdn.rsocket.frame;

import com.jauntsdn.rsocket.exceptions.RSocketException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import java.nio.charset.StandardCharsets;

public class ErrorFrameFlyweight {

  // defined error codes
  public static final int INVALID_SETUP = 0x00000001;
  public static final int UNSUPPORTED_SETUP = 0x00000002;
  public static final int REJECTED_SETUP = 0x00000003;
  public static final int REJECTED_RESUME = 0x00000004;
  public static final int CONNECTION_ERROR = 0x00000101;
  public static final int CONNECTION_CLOSE = 0x00000102;
  public static final int APPLICATION_ERROR = 0x00000201;
  public static final int REJECTED = 0x00000202;
  public static final int CANCELED = 0x00000203;
  public static final int INVALID = 0x00000204;

  public static ByteBuf encode(
      ByteBufAllocator allocator, int streamId, int errorCode, String errorMessage) {
    ByteBuf header = FrameHeaderFlyweight.encode(allocator, streamId, FrameType.ERROR, 0);
    String msg = errorMessage == null ? "" : errorMessage;
    ByteBuf msgBuf = ByteBufUtil.writeUtf8(allocator, msg);

    header.writeInt(errorCode);

    return allocator.compositeBuffer(2).addComponents(true, header, msgBuf);
  }

  public static ByteBuf encode(ByteBufAllocator allocator, int streamId, Throwable t) {
    return encode(allocator, streamId, errorCodeFromException(t), t.getMessage());
  }

  public static int errorCodeFromException(Throwable t) {
    if (t instanceof RSocketException) {
      return ((RSocketException) t).errorCode();
    }

    return APPLICATION_ERROR;
  }

  public static int errorCode(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size());
    int i = byteBuf.readInt();
    byteBuf.resetReaderIndex();
    return i;
  }

  public static ByteBuf data(ByteBuf byteBuf) {
    byteBuf.markReaderIndex();
    byteBuf.skipBytes(FrameHeaderFlyweight.size() + Integer.BYTES);
    ByteBuf slice = byteBuf.slice();
    byteBuf.resetReaderIndex();
    return slice;
  }

  public static String dataUtf8(ByteBuf byteBuf) {
    return data(byteBuf).toString(StandardCharsets.UTF_8);
  }
}
