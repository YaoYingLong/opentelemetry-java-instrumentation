/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.grpc.v1_6;

import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.extendsClass;
import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.hasClassesNamed;
import static io.opentelemetry.javaagent.instrumentation.grpc.v1_6.GrpcSingletons.MANAGED_CHANNEL_BUILDER_INSTRUMENTED;
import static net.bytebuddy.matcher.ElementMatchers.declaresField;
import static net.bytebuddy.matcher.ElementMatchers.named;

import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import java.util.List;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

/** 在客户端 channel 构建前自动安装追踪拦截器，使通过该 channel 发起的 RPC 进入 library 追踪链路。 */
class GrpcClientBuilderBuildInstrumentation implements TypeInstrumentation {
  @Override
  public ElementMatcher<ClassLoader> classLoaderOptimization() {
    return hasClassesNamed("io.grpc.ManagedChannelBuilder");
  }

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    // Advice 要直接调整 interceptors 列表，所以只匹配实际声明该字段的 builder 实现。
    return extendsClass(named("io.grpc.ManagedChannelBuilder"))
        .and(declaresField(named("interceptors")));
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(named("build"), getClass().getName() + "$AddInterceptorAdvice");
  }

  @SuppressWarnings("unused")
  public static class AddInterceptorAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static void addInterceptor(
        @Advice.This ManagedChannelBuilder<?> builder,
        @Advice.FieldValue("interceptors") List<ClientInterceptor> interceptors) {
      // 标记属于 builder 实例，防止重复调用 build 时再次添加相同的追踪组件。
      if (!Boolean.TRUE.equals(MANAGED_CHANNEL_BUILDER_INSTRUMENTED.get(builder))) {
        int existingInterceptors = interceptors.size();
        GrpcSingletons.addClientInterceptor(builder);
        if (interceptors.size() > existingInterceptors) {
          // 普通 intercept 路径会追加一个元素，将它移到列表首位，使其靠近底层 channel。
          // target-aware API 可能延迟创建拦截器；列表没有增长时不在这里移动元素。
          interceptors.add(0, interceptors.remove(existingInterceptors));
        }
        MANAGED_CHANNEL_BUILDER_INSTRUMENTED.set(builder, true);
      }
    }
  }
}
