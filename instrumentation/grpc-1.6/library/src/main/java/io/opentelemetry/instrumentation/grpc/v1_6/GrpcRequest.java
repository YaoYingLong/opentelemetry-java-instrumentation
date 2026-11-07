/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.opentelemetry.instrumentation.grpc.v1_6.internal.GrpcTargetParser;
import io.opentelemetry.instrumentation.grpc.v1_6.internal.ParsedTarget;
import java.net.SocketAddress;
import javax.annotation.Nullable;

/**
 * 一次 RPC 的遥测数据载体，供拦截器和属性提取器共享；它本身不是发送给远端的业务请求对象。
 *
 * <p>方法名和逻辑服务地址创建后不变，请求头、连接对端和消息大小则随调用阶段补充。后者使用 volatile 让不同执行线程可见最新值；请求/响应大小只保存最近一次赋值，不累计流中所有消息。
 */
public final class GrpcRequest {

  @Nullable private final MethodDescriptor<?, ?> method;
  private final String fullMethodName;
  @Nullable private final String originalFullMethodName;

  @Nullable private volatile Metadata metadata;
  @Nullable private final String serverAddress;
  @Nullable private final Integer serverPort;
  @Nullable private volatile SocketAddress peerSocketAddress;

  @Nullable private volatile Long requestSize;
  @Nullable private volatile Long responseSize;

  /**
   * Creates a client-side gRPC request.
   *
   * @param method the gRPC method descriptor
   * @param authority the channel authority (host:port)
   * @param parsedTarget the pre-parsed gRPC target (from {@link
   *     io.opentelemetry.instrumentation.grpc.v1_6.internal.GrpcTargetParser#parse}), or {@code
   *     null} if unavailable
   */
  static GrpcRequest createClientRequest(
      MethodDescriptor<?, ?> method,
      @Nullable String authority,
      @Nullable ParsedTarget parsedTarget) {
    // 优先使用配置目标；只有无法解析目标时才回退 authority，避免 authority override 覆盖真实的配置目标。
    ParsedTarget effective =
        parsedTarget != null ? parsedTarget : GrpcTargetParser.parseAuthority(authority);
    if (effective == null) {
      return new GrpcRequest(method, null, null, null, null);
    }
    return new GrpcRequest(method, null, null, effective.getAddress(), effective.getPort());
  }

  /**
   * Creates a server-side gRPC request.
   *
   * @param method the gRPC method descriptor
   * @param metadata the request metadata
   * @param peerSocketAddress the peer socket address
   * @param authority the request authority
   */
  static GrpcRequest createServerRequest(
      MethodDescriptor<?, ?> method,
      @Nullable Metadata metadata,
      @Nullable SocketAddress peerSocketAddress,
      @Nullable String authority) {
    // 服务端以请求 authority 表示逻辑服务，以传输属性中的 peerSocketAddress 表示调用方连接地址。
    ParsedTarget parsed = GrpcTargetParser.parseAuthority(authority);
    return new GrpcRequest(
        method,
        metadata,
        peerSocketAddress,
        parsed != null ? parsed.getAddress() : null,
        parsed != null ? parsed.getPort() : null);
  }

  private GrpcRequest(
      MethodDescriptor<?, ?> method,
      @Nullable Metadata metadata,
      @Nullable SocketAddress peerSocketAddress,
      @Nullable String serverAddress,
      @Nullable Integer serverPort) {
    this.method = method;
    this.fullMethodName = method.getFullMethodName();
    this.originalFullMethodName = null;
    this.metadata = metadata;
    this.peerSocketAddress = peerSocketAddress;
    this.serverAddress = serverAddress;
    this.serverPort = serverPort;
  }

  GrpcRequest(String fullMethodName, @Nullable String originalFullMethodName, Metadata metadata) {
    // 未注册方法没有 MethodDescriptor；stream tracer 传入归一化名称及原始请求名称。
    this.method = null;
    this.fullMethodName = fullMethodName;
    this.originalFullMethodName = originalFullMethodName;
    this.metadata = metadata;
    this.serverAddress = null;
    this.serverPort = null;
  }

  @Nullable
  public MethodDescriptor<?, ?> getMethod() {
    return method;
  }

  String getFullMethodName() {
    return fullMethodName;
  }

  @Nullable
  String getOriginalFullMethodName() {
    return originalFullMethodName;
  }

  @Nullable
  public Metadata getMetadata() {
    return metadata;
  }

  void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  /**
   * Returns the server address.
   *
   * <p>When a target string is available (from gRPC channel configuration), the server address is
   * extracted per the <a href="https://grpc.github.io/grpc/core/md_doc_naming.html">gRPC Name
   * Resolution spec</a>. Otherwise, falls back to the authority (host portion).
   */
  @Nullable
  public String getServerAddress() {
    return serverAddress;
  }

  /**
   * Returns the server port.
   *
   * <p>When a target string is available (from gRPC channel configuration), the server port is
   * extracted per the <a href="https://grpc.github.io/grpc/core/md_doc_naming.html">gRPC Name
   * Resolution spec</a>. Otherwise, falls back to the authority (port portion).
   */
  @Nullable
  public Integer getServerPort() {
    return serverPort;
  }

  @Nullable
  public SocketAddress getPeerSocketAddress() {
    return peerSocketAddress;
  }

  void setPeerSocketAddress(@Nullable SocketAddress peerSocketAddress) {
    this.peerSocketAddress = peerSocketAddress;
  }

  @Nullable
  public Long getRequestSize() {
    return requestSize;
  }

  public void setRequestSize(@Nullable Long requestSize) {
    this.requestSize = requestSize;
  }

  @Nullable
  public Long getResponseSize() {
    return responseSize;
  }

  public void setResponseSize(@Nullable Long responseSize) {
    this.responseSize = responseSize;
  }
}
