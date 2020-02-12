package com.jauntsdn.rsocket.integration;

import com.jauntsdn.rsocket.frame.FrameHeaderFlyweight;
import com.jauntsdn.rsocket.frame.FrameLengthFlyweight;
import com.jauntsdn.rsocket.transport.netty.client.TcpClientTransport;
import com.jauntsdn.rsocket.transport.netty.server.TcpServerTransport;

public class TcpPayloadSizeLimitTest extends PayloadSizeLimitTest {

  public TcpPayloadSizeLimitTest() {
    super(
        new Transport<>(
            TcpServerTransport.create("localhost", 0),
            TcpClientTransport::create,
            TcpPayloadSizeLimitTest::requestStreamPayloadLimit));
  }

  private static int requestStreamPayloadLimit(int frameSizeLimit) {
    return frameSizeLimit
        - (
        /*frame length for TCP transport*/
        FrameLengthFlyweight.FRAME_LENGTH_SIZE
            /*header length*/
            + FrameHeaderFlyweight.size()
            /*initial requestN*/
            + Integer.BYTES
            /*metadata length*/
            + 3);
  }
}
