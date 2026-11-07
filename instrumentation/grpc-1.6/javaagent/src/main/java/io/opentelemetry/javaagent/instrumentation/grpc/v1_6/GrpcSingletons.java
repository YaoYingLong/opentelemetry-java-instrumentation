/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.grpc.v1_6;

import io.grpc.Context;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ServerBuilder;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.incubator.config.DeclarativeConfigProperties;
import io.opentelemetry.instrumentation.api.config.IncludeExclude;
import io.opentelemetry.instrumentation.api.incubator.config.internal.DeclarativeConfigUtil;
import io.opentelemetry.instrumentation.api.util.VirtualField;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetryBuilder;
import io.opentelemetry.instrumentation.grpc.v1_6.internal.ContextStorageBridge;
import io.opentelemetry.instrumentation.grpc.v1_6.internal.Experimental;
import io.opentelemetry.instrumentation.grpc.v1_6.internal.GrpcConfig;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/** 为各处 Advice 共享遥测配置、builder 安装标记和上下文桥接器，避免每次 RPC 重复构造这些组件。 */
public class GrpcSingletons {

  public static final VirtualField<ManagedChannelBuilder<?>, Boolean>
      MANAGED_CHANNEL_BUILDER_INSTRUMENTED =
          VirtualField.find(ManagedChannelBuilder.class, Boolean.class);

  public static final VirtualField<ServerBuilder<?>, Boolean> SERVER_BUILDER_INSTRUMENTED =
      VirtualField.find(ServerBuilder.class, Boolean.class);

  private static final GrpcTelemetry telemetry;

  private static final AtomicReference<Context.Storage> storageReference = new AtomicReference<>();

  static {
    // Agent 使用全局 OpenTelemetry；配置在该 helper 初始化时读取，再交给 library builder 组装两端 instrumenter。
    OpenTelemetry openTelemetry = GlobalOpenTelemetry.get();
    DeclarativeConfigProperties config =
        DeclarativeConfigUtil.getInstrumentationConfig(openTelemetry, "grpc");
    GrpcConfig grpcConfig = GrpcConfig.create(openTelemetry);
    boolean emitMessageEvents = config.getBoolean("emit_message_events", true);

    boolean experimentalSpanAttributes =
        config.getBoolean("experimental_span_attributes/development", false);

    GrpcTelemetryBuilder telemetryBuilder =
        GrpcTelemetry.builder(openTelemetry)
            .setEmitMessageEvents(emitMessageEvents)
            .setCaptureExperimentalSpanAttributes(experimentalSpanAttributes);
    IncludeExclude clientRequestHeaders = grpcConfig.getClientRequestHeaders();
    if (clientRequestHeaders != null) {
      telemetryBuilder.setClientRequestHeaders(clientRequestHeaders);
    }

    IncludeExclude serverRequestHeaders = grpcConfig.getServerRequestHeaders();
    if (serverRequestHeaders != null) {
      telemetryBuilder.setServerRequestHeaders(serverRequestHeaders);
    }
    telemetry = telemetryBuilder.build();
  }

  @Nullable
  public static Context.Storage storage() {
    return storageReference.get();
  }

  public static void addClientInterceptor(ManagedChannelBuilder<?> builder) {
    Experimental.addClientInterceptor(telemetry, builder);
  }

  public static Context.Storage setStorage(Context.Storage storage) {
    // 首次访问 storage() 可能发生竞争，CAS 发布一个桥接器，所有调用方都返回已发布的同一实例。
    storageReference.compareAndSet(null, new ContextStorageBridge(storage));
    return storage();
  }

  public static void configureServerBuilder(ServerBuilder<?> serverBuilder) {
    telemetry.configureServerBuilder(serverBuilder);
  }

  private GrpcSingletons() {}
}
