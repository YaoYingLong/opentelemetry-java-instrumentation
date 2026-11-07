/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import io.opentelemetry.instrumentation.api.instrumenter.SpanNameExtractor;

// Small optimization to avoid RpcSpanNameExtractor because gRPC provides the span name directly.
// 已注册方法直接使用 package.Service/Method；未知方法的 _OTHER 标记映射为固定名称 grpc。
final class GrpcSpanNameExtractor implements SpanNameExtractor<GrpcRequest> {
  @Override
  public String extract(GrpcRequest request) {
    String fullMethodName = request.getFullMethodName();
    return fullMethodName.equals("_OTHER") ? "grpc" : fullMethodName;
  }
}
