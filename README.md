# RSocket

[RSocket](https://rsocket.io) is a binary session layer (L5) protocol & RPC on top of [Protocol Buffers](https://developers.google.com/protocol-buffers) for use on byte stream transports such as TCP, WebSockets and Http2.

* Rich symmetric (initiated by both Client and Server) interactions via async message passing, with flow control corresponding to [Reactive Streams](https://github.com/reactive-streams/reactive-streams-jvm) specification:
```
  request/channel (bidirectional stream)  
  request/stream (stream of many)  
  request/response (stream of 1)  
  fire-and-forget (no response)  
```
* Transport agnostic: RSockets can be carried by any connection-oriented protocol with reliable byte streams delivery - TCP, Websockets & Http2 are available OOTB 
* Built-in rate limiting with requests lease
* Automatic session resumption
* Efficient implementation of efficient protocol:
    * Stable and solid foundation by [netty](https://github.com/netty/netty) and [projectreactor](https://github.com/reactor/reactor-core): non-blocking IO, plus composable async operations with explicit flow control and error handling enabled excellent latency characteristics
    * Zero-copy message handling
    * Small core library footprint: suitable for both server and client/embedded applications
    
Server
```
 CloseableChannel server = 
     RSocketFactory.receive()
        .acceptor(new ServerAcceptor()) // (setup payload, requester RSocket) -> responder RSocket
        .transport(TcpServerTransport.create(7878))
        .start()
        .block();
```

Client
```
Mono<RSocket> client =
        RSocketFactory.connect()
            .setupPayload(payload)
            .acceptor(new OptionalClientAcceptor()) // (requester RSocket) -> responder RSocket 
            .transport(TcpClientTransport.create(7878))
            .start();
```

RSocket 
```
interface RSocket extends Availability, Closeable {
    Flux<Payload> requestChannel(Publisher<Payload> payloads);
    Flux<Payload> requestStream(Payload payload);
    Mono<Payload> requestResponse(Payload payload);
    Mono<Void> fireAndForget(Payload payload);
}
```

## RPC

Think GRPC (Clients & Servers stubs code generated from IDL with `Protocol Buffer` messages instead of plain bytes) with all features of RSocket: less chatty transports, request message-level flow control,
rate-limiting with request leasing, automatic session resumption - without leaking into application level APIs.   

IDL
```
service Service {
    rpc response (Request) returns (Response) {}
    rpc serverStream (Request) returns (stream Response) {}
    rpc clientStream (stream Request) returns (Response) {}
    rpc channel (stream Request) returns (stream Response) {}
}

message Request {
    string message = 1;
}

message Response {
    string message = 1;
}
```

RPC compiler generates Service interface
```
public interface StreamService {
  Mono<Response> response(Request message, ByteBuf metadata);
  Flux<Response> serverStream(Request message, ByteBuf metadata);
  Mono<Response> clientStream(Publisher<Request> messages, ByteBuf metadata);
  Flux<Response> channel(Publisher<Request> messages, ByteBuf metadata);
}
```
and Client/Server stubs
```
public ServiceServer(StreamService service, Optional<MeterRegistry> registry, Optional<Tracer> tracer) {

public ServiceClient(com.jauntsdn.rsocket.RSocket rSocket, MeterRegistry registry, Tracer tracer) {
```

## Build

Following command cleans output, formats sources, builds and installs binaries to local repository 
```
./gradlew clean build publishToMavenLocal
```

RSocket-RPC compiler is built separately, and requires [Protocol Buffers](https://github.com/grpc/grpc-java/blob/master/COMPILING.md#how-to-build-code-generation-plugin) compiler installed
```
cd rsocket-rpc-protobuf
./gradlew clean build
```

## Binaries

Releases are available via Maven Central.

##### RSocket

Bill of materials
```groovy
dependencyManagement {
        imports {
            mavenBom "com.jauntsdn.rsocket:rsocket-bom:<TBD>"
        }
}
```
Dependencies

```groovy
dependencies {
    implementation 'com.jauntsdn.rsocket:rsocket-core'
    implementation 'com.jauntsdn.rsocket:rsocket-transport-netty'
}
```

##### RSocket-RPC

Bill of materials
```groovy
dependencyManagement {
        imports {
            mavenBom "com.jauntsdn.rsocket:rsocket-rpc-bom:<TBD>"
        }
}
```
Dependencies

```groovy
dependencies {
    implementation 'com.jauntsdn.rsocket:rsocket-rpc-core'
}
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
