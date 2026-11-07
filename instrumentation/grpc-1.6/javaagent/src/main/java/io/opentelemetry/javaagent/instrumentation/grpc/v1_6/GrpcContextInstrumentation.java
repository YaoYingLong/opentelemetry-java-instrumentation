/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.grpc.v1_6;

import static io.opentelemetry.javaagent.instrumentation.grpc.v1_6.GrpcSingletons.storage;
import static net.bytebuddy.matcher.ElementMatchers.isStatic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.returns;

import io.grpc.Context;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import javax.annotation.Nullable;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.asm.Advice.AssignReturned;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

/** 替换 gRPC Context.storage() 的返回值，让 gRPC 的上下文切换同时携带 OpenTelemetry 上下文。 */
class GrpcContextInstrumentation implements TypeInstrumentation {
  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return named("io.grpc.Context");
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        isStatic().and(named("storage")).and(returns(named("io.grpc.Context$Storage"))),
        getClass().getName() + "$ContextBridgeAdvice");
  }

  @SuppressWarnings("unused")
  public static class ContextBridgeAdvice {

    @Advice.OnMethodEnter(
        skipOn = Advice.OnNonDefaultValue.class,
        suppress = Throwable.class,
        inline = false)
    @Nullable
    public static Context.Storage onEnter() {
      // 桥接器已初始化时返回非空值，skipOn 会跳过原 storage() 方法，退出 Advice 直接复用它。
      return storage();
    }

    @AssignReturned.ToReturned
    @Advice.OnMethodExit(suppress = Throwable.class, inline = false)
    public static Context.Storage onExit(
        @Advice.Return Context.Storage originalStorage,
        @Advice.Enter @Nullable Context.Storage ourStorage) {
      // 第一次调用让原方法正常返回，再包裹其 Storage；保留原存储用于判断 gRPC 自身的传播状态。
      return ourStorage != null ? ourStorage : GrpcSingletons.setStorage(originalStorage);
    }
  }
}
