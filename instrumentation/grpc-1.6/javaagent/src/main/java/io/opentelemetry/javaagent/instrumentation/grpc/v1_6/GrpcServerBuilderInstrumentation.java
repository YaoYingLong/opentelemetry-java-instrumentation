/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.grpc.v1_6;

import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.extendsClass;
import static io.opentelemetry.javaagent.extension.matcher.AgentElementMatchers.hasClassesNamed;
import static io.opentelemetry.javaagent.instrumentation.grpc.v1_6.GrpcSingletons.SERVER_BUILDER_INSTRUMENTED;
import static net.bytebuddy.matcher.ElementMatchers.isPublic;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.grpc.ServerBuilder;
import io.opentelemetry.javaagent.bootstrap.CallDepth;
import io.opentelemetry.javaagent.extension.instrumentation.TypeInstrumentation;
import io.opentelemetry.javaagent.extension.instrumentation.TypeTransformer;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

/** 在服务器构建前安装服务端拦截器和 stream tracer，分别覆盖已注册方法和未注册方法的请求。 */
class GrpcServerBuilderInstrumentation implements TypeInstrumentation {

  @Override
  public ElementMatcher<ClassLoader> classLoaderOptimization() {
    return hasClassesNamed("io.grpc.ServerBuilder");
  }

  @Override
  public ElementMatcher<TypeDescription> typeMatcher() {
    return extendsClass(named("io.grpc.ServerBuilder"));
  }

  @Override
  public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        isPublic().and(named("build")).and(takesArguments(0)),
        getClass().getName() + "$BuildAdvice");
  }

  @SuppressWarnings("unused")
  public static class BuildAdvice {

    @Advice.OnMethodEnter(suppress = Throwable.class, inline = false)
    public static CallDepth onEnter(@Advice.This ServerBuilder<?> serverBuilder) {
      // builder 的继承/委托实现可能嵌套调用 build；调用深度只允许最外层执行安装逻辑。
      CallDepth callDepth = CallDepth.forClass(ServerBuilder.class);
      if (callDepth.getAndIncrement() > 0) {
        return callDepth;
      }
      if (!Boolean.TRUE.equals(SERVER_BUILDER_INSTRUMENTED.get(serverBuilder))) {
        // 与调用深度不同，该标记处理同一个 builder 在不同时间被重复 build 的情况。
        GrpcSingletons.configureServerBuilder(serverBuilder);
        SERVER_BUILDER_INSTRUMENTED.set(serverBuilder, true);
      }
      return callDepth;
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class, inline = false)
    public static void onExit(@Advice.Enter CallDepth callDepth) {
      // 包括 build 抛异常的路径，都必须归还本次进入时增加的深度。
      callDepth.decrementAndGet();
    }
  }
}
