/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.grpc.v1_6.internal;

import static java.util.logging.Level.SEVERE;

import io.grpc.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;
import java.util.logging.Logger;

/**
 * {@link Context.Storage} override which uses OpenTelemetry context as the backing store. Both gRPC
 * and OpenTelemetry contexts refer to each other to ensure that both OTel context propagation
 * mechanisms and gRPC context propagation mechanisms can be used interchangeably.
 *
 * <p>桥接思路：OTel Context 通过 GRPC_CONTEXT 保存 gRPC Context，gRPC Context 通过 OTEL_CONTEXT 保存读取时的 OTel
 * Context。这样任意一侧捕获并传播上下文时，都可以携带另一侧的信息。 attach 返回的恢复上下文还保存本次打开的 Scope，detach 时用它恢复原线程状态。
 *
 * <p>This class is internal and is hence not for public use. Its APIs are unstable and can change
 * at any time.
 */
public final class ContextStorageBridge extends Context.Storage {

  private static final Logger logger = Logger.getLogger(ContextStorageBridge.class.getName());

  private static final ContextKey<Context> GRPC_CONTEXT = ContextKey.named("grpc-context");
  private static final Context.Key<io.opentelemetry.context.Context> OTEL_CONTEXT =
      Context.key("otel-context");
  private static final Context.Key<Scope> OTEL_SCOPE = Context.key("otel-scope");
  // context attached to original context store
  private static final Context.Key<Context> ORIGINAL_CONTEXT = Context.key("original-context");
  // context that should be restored in original context store on detach
  private static final Context.Key<Context> ORIGINAL_TO_RESTORE =
      Context.key("original-to-restore");

  private final boolean propagateGrpcDeadline;
  // original context storage that would have been used when running without agent
  private final Context.Storage originalStorage;

  public ContextStorageBridge(boolean propagateGrpcDeadline) {
    // library 的 ContextStorageOverride 使用此入口，不额外维护原 gRPC Storage。
    this.propagateGrpcDeadline = propagateGrpcDeadline;
    this.originalStorage = null;
  }

  public ContextStorageBridge(Context.Storage originalStorage) {
    // Agent 包裹已有 Storage，并在下方 current() 的特定跨上下文路径中隔离额外传播的 deadline/cancellation。
    propagateGrpcDeadline = false;
    this.originalStorage = originalStorage;
  }

  @Override
  public Context doAttach(Context toAttach) {
    // 旧 gRPC Context 从当前 OTel Context 读取；返回值将作为调用方稍后 detach 的 toRestore 参数。
    io.opentelemetry.context.Context otelContext = io.opentelemetry.context.Context.current();
    Context current = otelContext.get(GRPC_CONTEXT);

    if (current == null) {
      current = Context.ROOT;
    }

    if (current == toAttach) {
      // 已是同一个上下文时不再打开实际 Scope，但仍返回携带 noop Scope 的恢复信息。
      Context result = current.withValue(OTEL_SCOPE, Scope.noop());
      result = attachOriginalContextStorage(result);
      return result;
    }

    // 优先恢复 toAttach 捕获的 OTel Context；只有它没有关联值时才沿用执行线程当前的 OTel Context。
    io.opentelemetry.context.Context base = OTEL_CONTEXT.get(toAttach);
    io.opentelemetry.context.Context newOtelContext;
    if (base != null) {
      // gRPC context which has an OTel context associated with it via a call to
      // ContextStorageOverride.current(). Using it as the base allows it to be propagated together
      // with the gRPC context.
      newOtelContext = base.with(GRPC_CONTEXT, toAttach);
    } else {
      // gRPC context without an OTel context associated with it. This is only possible when
      // attaching a context directly created by Context.ROOT, e.g., Context.ROOT.with(...) which
      // is not common. We go ahead and assume the gRPC context can be reset while using the current
      // OTel context.
      newOtelContext = io.opentelemetry.context.Context.current().with(GRPC_CONTEXT, toAttach);
    }

    Scope scope = newOtelContext.makeCurrent();
    // Scope 存在“恢复用”的上下文中，而不是被 attach 的业务上下文中，使嵌套 attach/detach 各自配对。
    Context result = current.withValue(OTEL_SCOPE, scope);
    result = attachOriginalContextStorage(result);
    return result;
  }

  private Context attachOriginalContextStorage(Context context) {
    // Agent 模式同时维护原 Storage 的 attach 栈，并保存实际传给它的对象及其返回的旧对象。
    Context result = context;
    if (originalStorage != null) {
      Context originalToRestore = originalStorage.doAttach(result);
      result = result.withValues(ORIGINAL_CONTEXT, result, ORIGINAL_TO_RESTORE, originalToRestore);
    }
    return result;
  }

  @Override
  public void detach(Context toDetach, Context toRestore) {
    // 先恢复原 gRPC Storage，再关闭这次 attach 保存的 OTel Scope；二者都依赖 toRestore 中的配对信息。
    if (originalStorage != null) {
      Context originalContext = ORIGINAL_CONTEXT.get(toRestore);
      Context originalToRestore = ORIGINAL_TO_RESTORE.get(toRestore);
      originalStorage.detach(originalContext, originalToRestore);
    }

    Scope scope = OTEL_SCOPE.get(toRestore);
    if (scope == null) {
      logger.log(
          SEVERE, "Detaching context which was not attached.", new Throwable().fillInStackTrace());
    } else {
      scope.close();
    }
  }

  @Override
  public Context current() {
    // 每次读取都关联此刻的 OTel Context，保证在已有 gRPC Context 内新建 span 后再传播时能带上新 span。
    io.opentelemetry.context.Context otelContext = io.opentelemetry.context.Context.current();
    Context current = otelContext.get(GRPC_CONTEXT);
    if (current == null) {
      return Context.ROOT.withValue(OTEL_CONTEXT, otelContext);
    }
    // Store the current OTel context in the gRPC context so that gRPC context propagation
    // mechanisms will also propagate the OTel context.
    io.opentelemetry.context.Context previousOtelContext = OTEL_CONTEXT.get(current);
    if (previousOtelContext != otelContext) {
      // This context has already been previously attached and associated with an OTel context. Just
      // create a new context referring to the current OTel context to reflect the current stack.
      // The previous context is unaffected and will continue to live in its own stack.

      if (!propagateGrpcDeadline && originalStorage != null) {
        // 原 Storage 的当前上下文为 null 或 ROOT，表示这条路径没有同时通过 gRPC 自身传播上下文。
        // 此时 fork 保留键值但切断原取消/deadline 关系，避免 Agent 额外传播导致异步任务意外超时。
        Context originalCurrent = originalStorage.current();
        // check whether grpc context would have propagated without otel context
        if (originalCurrent == null || originalCurrent == Context.ROOT) {
          // Because we are propagating gRPC context via OpenTelemetry here, we may also propagate a
          // deadline where it wasn't present before. Notably, this could happen with no user
          // intention when using the javaagent which will add OpenTelemetry propagation
          // automatically, and cause that code to fail with a deadline cancellation. While ideally
          // we could propagate deadline as well as gRPC intended, we cannot have existing code fail
          // because it added the javaagent and choose to fork here.
          current = current.fork();
        }
      }

      return current.withValue(OTEL_CONTEXT, otelContext);
    }
    return current;
  }
}
