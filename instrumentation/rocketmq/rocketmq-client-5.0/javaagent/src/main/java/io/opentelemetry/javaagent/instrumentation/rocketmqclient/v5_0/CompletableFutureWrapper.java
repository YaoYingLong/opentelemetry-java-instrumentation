/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.javaagent.instrumentation.rocketmqclient.v5_0;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.concurrent.CompletableFuture;

/**
 * 在应用调用 sendAsync 的上下文中完成替代 Future，使同步触发的应用回调恢复正确的父上下文。
 *
 * <p>这里只控制完成动作所在的 Scope；已完成后才注册的回调，以及异步执行器上的后续阶段， 仍遵循 CompletableFuture
 * 自身的执行规则，不能仅靠这个包装器保证全部后续回调的上下文。
 */
public class CompletableFutureWrapper {

  private CompletableFutureWrapper() {}

  public static <T> CompletableFuture<T> wrap(CompletableFuture<T> future) {
    CompletableFuture<T> result = new CompletableFuture<>();
    Context context = Context.current();
    // 捕获的是包装时的调用方 Context；完成线程可能是 SDK 线程，执行完毕后需恢复其原有上下文。
    future.whenComplete(
        (T value, Throwable throwable) -> {
          try (Scope ignored = context.makeCurrent()) {
            if (throwable != null) {
              result.completeExceptionally(throwable);
            } else {
              result.complete(value);
            }
          }
        });

    return result;
  }
}
