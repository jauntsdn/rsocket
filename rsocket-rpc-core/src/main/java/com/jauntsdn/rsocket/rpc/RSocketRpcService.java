package com.jauntsdn.rsocket.rpc;

import com.jauntsdn.rsocket.Payload;
import com.jauntsdn.rsocket.ResponderRSocket;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public interface RSocketRpcService extends ResponderRSocket {
  String getService();

  Flux<Payload> requestChannel(Payload payload, Flux<Payload> publisher);

  default Flux<Payload> requestChannel(Payload payload, Publisher<Payload> payloads) {
    return requestChannel(payload, Flux.from(payloads));
  }
}
