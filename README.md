[![Build Status](https://travis-ci.org/jauntsdn/rsocket.svg?branch=develop)](https://travis-ci.org/jauntsdn/rsocket) ![Maven Central](https://img.shields.io/maven-central/v/com.jauntsdn.rsocket/rsocket-core)

# RSocket

Java implementation of [RSocket](https://rsocket.io) - binary session layer protocol for use on byte stream transports.  
It is intended for high-throughput applications targeting low latency, with peers connected by heterogeneous and probably unreliable networks. 

Notable features:

* Rich symmetric (initiated by both client and server) interactions via byte message passing, with flow control according to [Reactive Streams](https://github.com/reactive-streams/reactive-streams-jvm)
  
  `request/channel`  - bidirectional stream of many  
  `request/stream`   - stream of many  
  `request/response` - stream of 1  
  `fire-and-forget`  - no response  

* Concurrency limiting with requests leasing
* Automatic session resumption
* Transport agnostic: RSockets can be carried by any connection-oriented protocol with reliable byte streams delivery - TCP, WebSockets, Http2, Unix sockets etc
* Efficient implementation:
    * [Netty](https://github.com/netty/netty) and [ProjectReactor](https://github.com/reactor/reactor-core) for non-blocking IO and flow control
    * Zero-copy message handling
    * Small core library footprint: suitable for both server and client/embedded applications
* Accompanied by RSocket-RPC - efficient remote procedure call system on top of [Protocol Buffers](https://developers.google.com/protocol-buffers).

RSocket 
```java
interface RSocket extends Availability, Closeable {
    Flux<Payload> requestChannel(Publisher<Payload> payloads);
    Flux<Payload> requestStream(Payload payload);
    Mono<Payload> requestResponse(Payload payload);
    Mono<Void> fireAndForget(Payload payload);
}
```
    
Server
```java
 Mono<CloseableChannel> server = 
     RSocketFactory.receive()
         // connectionSetupPayload, requester RSocket        
        .acceptor((payload, rSocket) -> Mono.just(new ServerAcceptorRSocket(payload, rSocket)))
        .transport(TcpServerTransport.create(7878))
        .start();
```

Client
```java
Mono<RSocket> client =
        RSocketFactory.connect()
            .setupPayload(payload)
             // requester RSocket        
            .acceptor(rSocket -> new ClientAcceptorRSocket(rSocket))  
            .transport(TcpClientTransport.create(7878))
            .start();
```

## RSocket-RPC

Code generation based RPC system on top of `Protocol Buffers` with all features of RSocket:  
pluggable transports, request message-level flow control, concurrency limiting with request leasing, automatic session resumption.   

Services and messages are declared with Protocol Buffers IDL
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

Compiler generates Service interface
```java
public interface Service {
  Mono<Response> response(Request message, ByteBuf metadata);
  Flux<Response> serverStream(Request message, ByteBuf metadata);
  Mono<Response> clientStream(Publisher<Request> messages, ByteBuf metadata);
  Flux<Response> channel(Publisher<Request> messages, ByteBuf metadata);
}
```
and Server/Client stubs
```java
ServiceServer extends AbstractRSocket {
    public ServiceServer(StreamService service, 
                         Optional<ByteBufAllocator> allocator, 
                         Optional<MeterRegistry> registry, 
                         Optional<Tracer> tracer) {}
}
```

```java
ServiceClient implements Service {
    public ServiceClient(RSocket rSocket, 
                         Optional<ByteBufAllocator> allocator, 
                         Optional<MeterRegistry> registry, 
                         Optional<Tracer> tracer) {}
}
```

## Build

Building and publishing binaries to local Maven repository 
```
./gradlew clean build publishToMavenLocal
```

Building RSocket-RPC compiler requires [Protocol Buffers](https://github.com/grpc/grpc-java/blob/master/COMPILING.md#how-to-build-code-generation-plugin) compiler installed
```
cd rsocket-rpc-protobuf
./gradlew clean build publishToMavenLocal
```

## Examples 

Runnable examples are available at [rsocket-showcases](https://github.com/jauntsdn/rsocket-showcases) repository.

## Binaries

Releases are available via Maven Central.

##### RSocket

Bill of materials
```groovy
dependencyManagement {
        imports {
            mavenBom "com.jauntsdn.rsocket:rsocket-bom:0.9.7"
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
            mavenBom "com.jauntsdn.rsocket:rsocket-rpc-bom:0.9.7"
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
