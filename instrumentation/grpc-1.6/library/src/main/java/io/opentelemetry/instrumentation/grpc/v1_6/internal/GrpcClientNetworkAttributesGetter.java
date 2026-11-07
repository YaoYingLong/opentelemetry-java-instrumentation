/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6.internal;

import io.grpc.Status;
import io.opentelemetry.instrumentation.api.semconv.network.NetworkAttributesGetter;
import io.opentelemetry.instrumentation.api.semconv.network.ServerAttributesGetter;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcRequest;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import javax.annotation.Nullable;

/**
 * This class is internal and is hence not for public use. Its APIs are unstable and can change at
 * any time.
 *
 * <p>客户端的逻辑 server 地址来自配置 target/authority，实际 network.peer 地址由调用结束时的传输属性提供。
 * 两者可能因名字解析、代理或负载均衡而不同，分别保留各自含义。
 */
public final class GrpcClientNetworkAttributesGetter
    implements ServerAttributesGetter<GrpcRequest>, NetworkAttributesGetter<GrpcRequest, Status> {

  @Nullable
  @Override
  public String getServerAddress(GrpcRequest grpcRequest) {
    return grpcRequest.getServerAddress();
  }

  @Override
  @Nullable
  public Integer getServerPort(GrpcRequest grpcRequest) {
    return grpcRequest.getServerPort();
  }

  @Override
  @Nullable
  public InetSocketAddress getNetworkPeerInetSocketAddress(
      GrpcRequest request, @Nullable Status response) {
    SocketAddress address = request.getPeerSocketAddress();
    // 只为 IP socket 暴露 InetSocketAddress；其他传输类型或尚未获取地址时返回 null。
    if (address instanceof InetSocketAddress) {
      return (InetSocketAddress) address;
    }
    return null;
  }
}
