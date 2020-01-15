package com.jauntsdn.rsocket.frame;

import static org.junit.jupiter.api.Assertions.*;

import com.jauntsdn.rsocket.exceptions.ApplicationErrorException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.junit.jupiter.api.Test;

class ErrorFrameFlyweightTest {
  @Test
  void testEncode() {
    ByteBuf frame =
        ErrorFrameFlyweight.encode(ByteBufAllocator.DEFAULT, 1, new ApplicationErrorException("d"));

    frame = FrameLengthFlyweight.encode(ByteBufAllocator.DEFAULT, frame.readableBytes(), frame);
    assertEquals("00000b000000012c000000020164", ByteBufUtil.hexDump(frame));
    frame.release();
  }
}
