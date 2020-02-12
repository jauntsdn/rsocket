package com.jauntsdn.rsocket.integration;

import com.jauntsdn.rsocket.frame.FrameHeaderFlyweight;
import com.jauntsdn.rsocket.transport.netty.client.WebsocketClientTransport;
import com.jauntsdn.rsocket.transport.netty.server.WebsocketServerTransport;

public class WebSocketPayloadSizeLimitTest extends PayloadSizeLimitTest {
  public WebSocketPayloadSizeLimitTest() {
    super(
        new Transport<>(
            WebsocketServerTransport.create("localhost", 0),
            WebsocketClientTransport::create,
            WebSocketPayloadSizeLimitTest::requestStreamPayloadLimit));
  }

  private static int requestStreamPayloadLimit(int frameSizeLimit) {
    return frameSizeLimit
        - (
        /*header length*/
        +FrameHeaderFlyweight.size()
            /*initial requestN*/
            + Integer.BYTES
            /*metadata length*/
            + 3);
  }
}
