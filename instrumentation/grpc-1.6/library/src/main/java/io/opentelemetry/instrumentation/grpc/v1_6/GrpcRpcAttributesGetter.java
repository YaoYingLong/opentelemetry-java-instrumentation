/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import io.grpc.Metadata;
import io.grpc.Status;
import io.opentelemetry.instrumentation.api.incubator.semconv.rpc.RpcAttributesGetter;
import java.util.List;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

/** 把 gRPC 请求/结果适配为通用 RPC 语义字段；属性名、写入时机和指标处理由对应的 RPC 提取器负责。 */
final class GrpcRpcAttributesGetter implements RpcAttributesGetter<GrpcRequest, Status> {

  private final boolean isClient;

  GrpcRpcAttributesGetter(boolean isClient) {
    this.isClient = isClient;
  }

  @Override
  public String getRpcSystemName(GrpcRequest request) {
    return "grpc";
  }

  @Override
  public String getRpcMethod(GrpcRequest request) {
    return request.getFullMethodName();
  }

  @Override
  @Nullable
  public Long getRequestSize(GrpcRequest request) {
    return request.getRequestSize();
  }

  @Override
  @Nullable
  public Long getResponseSize(GrpcRequest request) {
    return request.getResponseSize();
  }

  @Override
  @Nullable
  public String getRpcMethodOriginal(GrpcRequest request) {
    return request.getOriginalFullMethodName();
  }

  @Override
  @Nullable
  public String getErrorType(
      GrpcRequest request, @Nullable Status response, @Nullable Throwable error) {
    // 协议状态被当前角色判为错误时使用状态名称；否则仅在有异常时使用异常类名。
    if (response != null
        && (isClient ? !response.isOk() : GrpcSpanStatusExtractor.isServerError(response))) {
      return response.getCode().name();
    }
    return error == null ? null : error.getClass().getName();
  }

  List<String> metadataValue(GrpcRequest request, String key) {
    // 此方法读取 ASCII 多值头，调用方 GrpcAttributesExtractor 已过滤二进制头和非法名称。
    if (request.getMetadata() == null) {
      return emptyList();
    }

    if (key.isEmpty()) {
      return emptyList();
    }

    Iterable<String> values =
        request.getMetadata().getAll(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER));

    if (values == null) {
      return emptyList();
    }

    return StreamSupport.stream(values.spliterator(), false).collect(toList());
  }
}
