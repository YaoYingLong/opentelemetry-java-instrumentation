/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6;

import static java.util.Objects.requireNonNull;

import io.grpc.ClientInterceptor;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.instrumentation.api.instrumenter.Instrumenter;
import io.opentelemetry.instrumentation.grpc.v1_6.internal.Internal;
import javax.annotation.Nullable;

/**
 * Entrypoint for instrumenting gRPC servers or clients.
 *
 * <p>library 的接入入口：手动接入时由应用把拦截器装到 gRPC builder，自动插桩时由 GrpcSingletons 完成安装。 本对象持有可复用的 instrumenter
 * 和传播器，每次 RPC 的独立状态由各拦截器创建的包装器持有。
 */
public final class GrpcTelemetry {

  static {
    // 将包内创建逻辑提供给 internal 包；这个入口接收 gRPC 已计算的 target，保留与原始 target 的语义区别。
    Internal.setClientInterceptorFactory(
        (telemetry, target) -> telemetry.newTracingClientInterceptor(target, true));
  }

  private final Instrumenter<GrpcRequest, Status> serverInstrumenter;
  private final Instrumenter<GrpcRequest, Status> clientInstrumenter;
  private final ContextPropagators propagators;
  private final boolean captureExperimentalSpanAttributes;
  private final boolean emitMessageEvents;

  /** Returns a new {@link GrpcTelemetry} configured with the given {@link OpenTelemetry}. */
  public static GrpcTelemetry create(OpenTelemetry openTelemetry) {
    return builder(openTelemetry).build();
  }

  /** Returns a new {@link GrpcTelemetryBuilder} configured with the given {@link OpenTelemetry}. */
  public static GrpcTelemetryBuilder builder(OpenTelemetry openTelemetry) {
    return new GrpcTelemetryBuilder(openTelemetry);
  }

  GrpcTelemetry(
      Instrumenter<GrpcRequest, Status> serverInstrumenter,
      Instrumenter<GrpcRequest, Status> clientInstrumenter,
      ContextPropagators propagators,
      boolean captureExperimentalSpanAttributes,
      boolean emitMessageEvents) {
    this.serverInstrumenter = serverInstrumenter;
    this.clientInstrumenter = clientInstrumenter;
    this.propagators = propagators;
    this.captureExperimentalSpanAttributes = captureExperimentalSpanAttributes;
    this.emitMessageEvents = emitMessageEvents;
  }

  /**
   * Returns a new {@link ClientInterceptor} for use with methods like {@link
   * io.grpc.ManagedChannelBuilder#intercept(ClientInterceptor...)}.
   *
   * <p>Uses the channel's authority for {@code server.address} and {@code server.port}. Use {@link
   * #createClientInterceptor(String)} when the configured channel target is available.
   */
  public ClientInterceptor createClientInterceptor() {
    // 没有 target 时，后续从 Channel.authority() 提取服务地址。
    return newTracingClientInterceptor(null, false);
  }

  /**
   * Returns a new {@link ClientInterceptor} that uses the configured channel target for {@code
   * server.address} and {@code server.port}.
   *
   * <p>Pass the same target used to configure the channel, not an authority override. For custom
   * name resolvers, use an explicit {@code scheme://...} target to distinguish it from a {@code
   * host:port} address. If the target does not identify a usable endpoint, the interceptor falls
   * back to the channel's authority.
   *
   * <p>This method does not depend on gRPC's internal target-aware interception API.
   *
   * @param target the configured gRPC channel target, for example {@code dns:///host:8443}
   */
  public ClientInterceptor createClientInterceptor(String target) {
    // 这里的 target 是应用配置 channel 时使用的原始字符串，不是 overrideAuthority 的替代值。
    return newTracingClientInterceptor(requireNonNull(target, "target"), false);
  }

  /**
   * Configures a {@link ServerBuilder} with the server interceptor, which handles registered
   * service methods.
   *
   * <p>Also registers a stream tracer factory that creates spans for requests to unregistered
   * services, which server interceptors never see.
   */
  public void configureServerBuilder(ServerBuilder<?> serverBuilder) {
    // 两个入口互补：拦截器追踪正常分发的 RPC，stream tracer 补充“找不到服务方法”的失败请求。
    serverBuilder.intercept(createServerInterceptor());
    serverBuilder.addStreamTracerFactory(
        new TracingServerStreamTracerFactory(serverInstrumenter, propagators));
  }

  /**
   * Returns a new {@link ServerInterceptor} for use with methods like {@link
   * io.grpc.ServerBuilder#intercept(ServerInterceptor)} and {@link
   * io.grpc.ServerInterceptors#intercept(io.grpc.ServerServiceDefinition, ServerInterceptor...)}.
   *
   * <p>An interceptor on its own does not see requests to services that are not registered on the
   * server. Prefer {@link #configureServerBuilder(ServerBuilder)} where a {@link ServerBuilder} is
   * available, because it can also register the stream tracer factory that captures those requests.
   */
  public ServerInterceptor createServerInterceptor() {
    return new TracingServerInterceptor(
        serverInstrumenter, captureExperimentalSpanAttributes, emitMessageEvents);
  }

  private TracingClientInterceptor newTracingClientInterceptor(
      @Nullable String target, boolean computedTarget) {
    return new TracingClientInterceptor(
        clientInstrumenter,
        propagators,
        captureExperimentalSpanAttributes,
        emitMessageEvents,
        target,
        computedTarget);
  }
}
