/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.grpc.v1_6;

import static java.util.Arrays.asList;

import com.google.auto.service.AutoService;
import io.opentelemetry.javaagent.extension.instrumentation.InstrumentationModule;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import java.util.List;

/**
 * gRPC 自动插桩的注册入口，1.6 表示模块支持的起始版本。
 *
 * <p>客户端/服务端 builder 插桩负责安装 library 提供的追踪组件，Context 插桩负责衔接两套上下文； RPC 的具体属性、事件和 span 生命周期由 library
 * 模块实现。
 */
@AutoService(InstrumentationModule.class)
public class GrpcInstrumentationModule extends InstrumentationModule {
  public GrpcInstrumentationModule() {
    super("grpc-1.6", "grpc");
  }

  @Override
  public List<TypeInstrumentation> typeInstrumentations() {
    return asList(
        new GrpcClientBuilderBuildInstrumentation(),
        new GrpcContextInstrumentation(),
        new GrpcServerBuilderInstrumentation());
  }
}
