# RSocket

RSocket is a binary protocol for use on byte stream transports such as TCP, WebSockets and Http2.

It enables the following symmetric interaction models via async message passing over a single connection:

- request/response (stream of 1)
- request/stream (stream of many)
- fire-and-forget (no response)
- request/channel (bidirectional stream)

Learn more at http://rsocket.io

## Build and Binaries

Releases are available via Maven Central.

Example:

```groovy
dependencies {
    implementation 'com.jauntsdn.rsocket:rsocket-core:<TBD>'
    implementation 'com.jauntsdn.rsocket:rsocket-transport-netty:<TBD>'
}
```


## Development

Install the google-java-format in Intellij, from Plugins preferences.
Enable under Preferences -> Other Settings -> google-java-format Settings

Format automatically with

```
$./gradlew goJF
```

## Debugging
Frames can be printed out to help debugging. Set the logger `com.jauntsdn.rsocket.FrameLogger` to debug to print the frames.

## Trivial Client

```java
package com.jauntsdn.rsocket.transport.netty;

import com.jauntsdn.rsocket.Payload;
import com.jauntsdn.rsocket.RSocket;
import com.jauntsdn.rsocket.RSocketFactory;
import com.jauntsdn.rsocket.transport.netty.client.WebsocketClientTransport;
import com.jauntsdn.rsocket.util.DefaultPayload;
import reactor.core.publisher.Flux;

import java.net.URI;

public class ExampleClient {
    public static void main(String[] args) {
        WebsocketClientTransport ws = WebsocketClientTransport.create(URI.create("ws://rsocket-demo.herokuapp.com/ws"));
        RSocket client = RSocketFactory.connect().transport(ws).start().block();

        try {
            Flux<Payload> response = client.requestStream(DefaultPayload.create("peace"));

            response.take(10).doOnNext(p -> {
               String data = p.getDataUtf8();
               p.release();
               System.out.println(data);            
            }).blockLast();
        } finally {
            client.dispose();
        }
    }
}
```

## Zero Copy
By default to make RSocket easier to use it copies the incoming Payload. Copying the payload comes at cost to performance
and latency. If you want to use zero copy you must disable this. To disable copying you must include a `payloadDecoder`
argument in your `RSocketFactory`. This will let you manage the Payload without copying the data from the underlying
transport. You must free the Payload when you are done with them
or you will get a memory leak. Used correctly this will reduce latency and increase performance.

### Example Server setup
```java
RSocketFactory.receive()
        // Enable Zero Copy
        .frameDecoder(PayloadDecoder.ZERO_COPY)
        .acceptor(new PingHandler())
        .transport(TcpServerTransport.create(7878))
        .start()
        .block()
        .onClose()
        .block();
```

### Example Client setup
```java
Mono<RSocket> client =
        RSocketFactory.connect()
            // Enable Zero Copy
            .frameDecoder(PayloadDecoder.ZERO_COPY)
            .transport(TcpClientTransport.create(7878))
            .start();
```

## Bugs and Feedback

For bugs, questions and discussions please use the [Github Issues](https://github.com/jauntsdn/rsocket/issues).

## LICENSE

Copyright 2015-2020 the original author or authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
