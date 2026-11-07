/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import io.grpc.Metadata;
import io.grpc.ServerStreamTracer;
import io.grpc.Status;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;

/** 在传输流创建时保存请求信息，为不会进入服务端拦截器的未知方法请求提供追踪入口。 */
final class TracingServerStreamTracerFactory extends ServerStreamTracer.Factory {

  private final Instrumenter<GrpcRequest, Status> instrumenter;
  private final ContextPropagators propagators;

  TracingServerStreamTracerFactory(
      Instrumenter<GrpcRequest, Status> instrumenter, ContextPropagators propagators) {
    this.instrumenter = instrumenter;
    this.propagators = propagators;
  }

  @Override
  public ServerStreamTracer newServerStreamTracer(String fullMethodName, Metadata headers) {
    // headers 的可读期限仅限本次工厂调用；复制后由 tracer 持有，延迟到 streamClosed 时提取传播信息。
    // gRPC does not allow reading the given headers after this method returns, so keep a copy that
    // the tracer owns.
    Metadata headersCopy = new Metadata();
    headersCopy.merge(headers);
    return new TracingServerStreamTracer(
        instrumenter, propagators, fullMethodName, headersCopy, Context.current());
  }
}
