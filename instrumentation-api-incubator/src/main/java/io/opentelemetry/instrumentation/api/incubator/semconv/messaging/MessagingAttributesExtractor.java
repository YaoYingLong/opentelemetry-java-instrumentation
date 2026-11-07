/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.instrumentation.api.incubator.semconv.messaging;

import static io.opentelemetry.semconv.ErrorAttributes.ERROR_TYPE;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.api.config.IncludeExclude;
import io.opentelemetry.instrumentation.api.instrumenter.AttributesExtractor;
import io.opentelemetry.instrumentation.api.internal.SpanKey;
import io.opentelemetry.instrumentation.api.internal.SpanKeyProvider;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Extractor of <a
 * href="https://github.com/open-telemetry/semantic-conventions/blob/v1.43.0/docs/messaging/messaging-spans.md">messaging
 * attributes</a>.
 *
 * <p>This class delegates to a type-specific {@link MessagingAttributesGetter} for individual
 * attribute extraction from request/response objects.
 *
 * <p>这是各消息客户端共用的属性映射层：Getter 负责从具体 SDK 的请求/响应中取值，本类负责把值写入 标准消息属性。例如 RocketMQ 的 Getter 从消息中取得
 * Topic，这里将其写为 {@code messaging.destination.name}。 {@code REQUEST} 可以是单条消息、批次包装对象或协议请求，{@code
 * RESPONSE} 则由对应操作决定。
 *
 * <p>Instrumenter 在开始操作时调用 {@link #onStart}，写入系统、目标和操作信息；结束时调用 {@link #onEnd}，补充消息
 * ID、批次数量、错误类型和选中的消息头。各阶段使用的属性构建器由 Instrumenter 提供，本类不保存某次请求的属性状态，也不负责创建/结束 Span 或注入传播上下文。
 *
 * <p>消息头采集表示把业务消息头复制为遥测属性，与向消息注入 {@code traceparent} 等传播字段是独立的机制。
 * 构造时预处理选择规则，处理消息时再读取实际值，兼顾精确名称查询和通配符筛选两种路径。
 */
public final class MessagingAttributesExtractor<REQUEST, RESPONSE>
    implements AttributesExtractor<REQUEST, RESPONSE>, SpanKeyProvider {

  // copied from MessagingIncubatingAttributes
  // 属性键同时确定输出名称和 Java 值类型；这些通用字段由各客户端的 Getter 提供值。
  private static final AttributeKey<Long> MESSAGING_BATCH_MESSAGE_COUNT =
      AttributeKey.longKey("messaging.batch.message_count");
  private static final AttributeKey<String> MESSAGING_CLIENT_ID =
      AttributeKey.stringKey("messaging.client.id");
  private static final AttributeKey<Boolean> MESSAGING_DESTINATION_ANONYMOUS =
      AttributeKey.booleanKey("messaging.destination.anonymous");
  private static final AttributeKey<String> MESSAGING_DESTINATION_NAME =
      AttributeKey.stringKey("messaging.destination.name");
  private static final AttributeKey<String> MESSAGING_DESTINATION_PARTITION_ID =
      AttributeKey.stringKey("messaging.destination.partition.id");
  private static final AttributeKey<String> MESSAGING_DESTINATION_SUBSCRIPTION_NAME =
      AttributeKey.stringKey("messaging.destination.subscription.name");
  // destination.name 是具体目标名；template 是 Getter 能提供时使用的目标模板，两者在这里分别提取。
  private static final AttributeKey<String> MESSAGING_DESTINATION_TEMPLATE =
      AttributeKey.stringKey("messaging.destination.template");
  private static final AttributeKey<Boolean> MESSAGING_DESTINATION_TEMPORARY =
      AttributeKey.booleanKey("messaging.destination.temporary");
  private static final AttributeKey<String> MESSAGING_MESSAGE_CONVERSATION_ID =
      AttributeKey.stringKey("messaging.message.conversation_id");
  // conversation_id 表示消息所属会话/关联组，message.id 标识消息；两者均不是追踪系统的 TraceId。
  private static final AttributeKey<String> MESSAGING_MESSAGE_ID =
      AttributeKey.stringKey("messaging.message.id");
  private static final AttributeKey<String> MESSAGING_OPERATION_NAME =
      AttributeKey.stringKey("messaging.operation.name");
  private static final AttributeKey<String> MESSAGING_OPERATION_TYPE =
      AttributeKey.stringKey("messaging.operation.type");
  private static final AttributeKey<String> MESSAGING_SYSTEM_NAME =
      AttributeKey.stringKey("messaging.system.name");

  /**
   * Creates the messaging attributes extractor for the given operation type.
   *
   * <p>便捷入口使用 Builder 的默认设置，不启用消息头采集。需要选择消息头时，通过 {@link #builder} 调用 {@link
   * MessagingAttributesExtractorBuilder#setHeaders} 后再构建。
   *
   * @param operationName the system-specific name of the operation, emitted as {@code
   *     messaging.operation.name}, e.g. {@code send}, {@code poll} or {@code ack}.
   */
  public static <REQUEST, RESPONSE> AttributesExtractor<REQUEST, RESPONSE> create(
      MessagingAttributesGetter<REQUEST, RESPONSE> getter,
      MessagingOperationType operationType,
      String operationName) {
    return builder(getter, operationType, operationName).build();
  }

  /**
   * Returns a new {@link MessagingAttributesExtractorBuilder} configured for the given operation
   * type.
   *
   * <p>三个参数由具体客户端插桩的工厂装配：{@code getter} 适配 SDK 数据结构，{@code operationType} 表示通用操作类别，{@code
   * operationName} 表示客户端的实际操作名。例如拉取消息可以使用 RECEIVE 类别和 {@code poll} 名称；RocketMQ 的接收则使用 RECEIVE 和
   * {@code receive}。
   *
   * @param operationName the system-specific name of the operation, emitted as {@code
   *     messaging.operation.name}, e.g. {@code send}, {@code poll} or {@code ack}.
   */
  public static <REQUEST, RESPONSE> MessagingAttributesExtractorBuilder<REQUEST, RESPONSE> builder(
      MessagingAttributesGetter<REQUEST, RESPONSE> getter,
      MessagingOperationType operationType,
      String operationName) {
    return new MessagingAttributesExtractorBuilder<>(
        getter,
        requireNonNull(operationType, "operationType"),
        requireNonNull(operationName, "operationName"));
  }

  // 以下字段在构造后保持不变，可随 Instrumenter 复用；运行时需要的临时集合在方法内创建。
  private final MessagingAttributesGetter<REQUEST, RESPONSE> getter;
  private final MessagingOperationType operationType;
  private final String operationName;
  // Builder.setHeaders 已把“包含和排除规则都为空”的选择器转为 null；null 表示不采集消息头。
  @Nullable private final IncludeExclude headers;
  // 已通过排除规则检查的精确名称，可直接调用 getMessageHeader(request, name)，无需枚举所有名称。
  // 即使 Getter 不提供可枚举的消息头名称，精确查询路径仍然可用。
  private final List<String> exactHeaderNames;
  // 精确名称对应的 messaging.header.<name> 键在构造时生成，避免每条消息都重新创建。
  private final Map<String, AttributeKey<List<String>>> exactHeaderAttributeKeys;
  // 通配符或仅排除配置无法预先列出全部名称，需要在每次操作结束时枚举实际消息头。
  private final boolean enumerateHeaderNames;

  /**
   * 由 Builder 创建，预先把消息头选择规则分成“精确查询”与“运行时枚举”两部分。
   *
   * <p>这里只处理配置，不读取具体消息；消息头的实际值在 onEnd 中获取，避免在开始和结束时重复采集。
   */
  MessagingAttributesExtractor(
      MessagingAttributesGetter<REQUEST, RESPONSE> getter,
      MessagingOperationType operationType,
      String operationName,
      @Nullable IncludeExclude headers) {
    this.getter = getter;
    this.operationType = operationType;
    this.operationName = operationName;
    this.headers = headers;

    // 去掉配置中的重复精确名称，同时保留第一次出现的顺序。
    Set<String> exactNames = new LinkedHashSet<>();
    boolean enumerate = false;
    if (headers != null) {
      /*
       * 例如公共配置可以选择 order-id，以及所有 tenant- 开头的消息头：
       * otel.instrumentation.common.messaging.headers.included=order-id,tenant-*
       * otel.instrumentation.common.messaging.headers.excluded=internal-*
       * 配置读取由调用方完成，本类接收的是已经构建好的 IncludeExclude。
       */
      List<String> included = headers.getIncluded();
      // 非空选择器没有 included，表示“采集所有未被 excluded 排除的消息头”，因此必须枚举名称。
      // 完全未配置的情形已在 Builder 归一化为 null，不会在这里被误当作“采集全部”。
      enumerate = included.isEmpty();
      for (String pattern : included) {
        if (pattern.indexOf('*') != -1 || pattern.indexOf('?') != -1) {
          // tenant-* 是匹配规则，不是真实消息头名称；等拿到具体请求后再逐个名称匹配。
          enumerate = true;
        } else if (headers.matches(pattern)) {
          // 即使 included 明确列出某个名称，也要经过完整选择器检查，因为 excluded 优先。
          exactNames.add(pattern);
        }
      }
    }
    // 后续请求只读取这份配置列表；运行时发现的名称不会追加到列表或长期缓存中。
    this.exactHeaderNames = unmodifiableList(new ArrayList<>(exactNames));
    this.exactHeaderAttributeKeys =
        CapturedMessageHeadersUtil.createLiteralAttributeKeys(exactHeaderNames);
    this.enumerateHeaderNames = enumerate;
  }

  /**
   * 提取开始阶段的请求属性。此回调只接收请求参数，消息 ID、实际批次数量和错误统一留到 onEnd 中提取。
   *
   * <p>Getter 返回 null 表示该字段不可获得或不适用，AttributesBuilder 会忽略它；这里不推测默认值。 parentContext
   * 是生命周期接口提供的父上下文，本提取器只映射属性，不改变父子关系。
   */
  @Override
  public void onStart(AttributesBuilder attributes, Context parentContext, REQUEST request) {
    attributes.put(MESSAGING_SYSTEM_NAME, getter.getSystem(request));
    // 临时/匿名标记仅在为 true 时输出，false 时省略属性；目标名称是否可用仍由 Getter 决定。
    boolean isTemporaryDestination = getter.isTemporaryDestination(request);
    if (isTemporaryDestination) {
      attributes.put(MESSAGING_DESTINATION_TEMPORARY, true);
    }
    attributes.put(MESSAGING_DESTINATION_NAME, getter.getDestination(request));
    attributes.put(MESSAGING_DESTINATION_TEMPLATE, getter.getDestinationTemplate(request));
    attributes.put(MESSAGING_DESTINATION_PARTITION_ID, getter.getDestinationPartitionId(request));
    boolean isAnonymousDestination = getter.isAnonymousDestination(request);
    if (isAnonymousDestination) {
      attributes.put(MESSAGING_DESTINATION_ANONYMOUS, true);
    }
    // 会话、客户端和订阅分别描述消息关联、客户端身份和消费订阅，不从 Topic 或消息 ID 派生。
    attributes.put(MESSAGING_MESSAGE_CONVERSATION_ID, getter.getConversationId(request));
    attributes.put(MESSAGING_CLIENT_ID, getter.getClientId(request));
    attributes.put(
        MESSAGING_DESTINATION_SUBSCRIPTION_NAME, getter.getDestinationSubscriptionName(request));
    // name 保留工厂传入的 API 操作名；type 使用统一枚举值，方便跨客户端按 send/receive/process 等聚合。
    attributes.put(MESSAGING_OPERATION_NAME, operationName);
    attributes.put(MESSAGING_OPERATION_TYPE, operationType.value());
  }

  /**
   * 提取操作结束时的结果属性，并按配置采集消息头；由 Instrumenter 合并到 Span 并交给相关操作监听器。
   *
   * <p>response 和 error 都可能为 null。Getter 根据具体协议决定如何从请求、响应或异常取得结果， 例如发送回执提供消息
   * ID、接收响应提供消息数量，或消费返回值表示没有抛异常的业务失败。
   */
  @Override
  public void onEnd(
      AttributesBuilder attributes,
      Context context,
      REQUEST request,
      @Nullable RESPONSE response,
      @Nullable Throwable error) {
    // 消息 ID 可能只有响应回来后才有；批次操作也可以返回 null，把各条消息的 ID 留给 Link 记录。
    attributes.put(MESSAGING_MESSAGE_ID, getter.getMessageId(request, response));
    // 批次大小由 Getter 明确提供；本类不把“未提供数量”自行解释成 0 或 1。
    attributes.put(MESSAGING_BATCH_MESSAGE_COUNT, getter.getBatchMessageCount(request, response));
    // 默认 Getter 返回异常类名，也可根据业务响应覆盖；这里只写 error.type，Span 状态由状态提取器决定。
    attributes.put(ERROR_TYPE, getter.getErrorType(request, response, error));

    for (String name : headerNames(request)) {
      // 接口约定：不存在该消息头时返回空列表，不能返回 null；一个消息头可以对应多个字符串值。
      List<String> values = getter.getMessageHeader(request, name);
      if (!values.isEmpty()) {
        // 输出为字符串数组属性，例如 messaging.header.order-id=["42"]。
        // 名称保留原有拼写和连字符；精确名称复用预生成的键，动态名称按需生成但不放入共享缓存。
        attributes.put(
            CapturedMessageHeadersUtil.attributeKey(name, exactHeaderAttributeKeys), values);
      }
    }
  }

  /**
   * 生成本次请求要查询的消息头名称：先保留配置中的精确名称，再按需加入枚举得到的匹配名称。
   *
   * <p>返回的是候选名称，精确配置中的名称可以在当前消息上不存在，随后由 getMessageHeader 返回空列表处理。 通配符筛选由 IncludeExclude
   * 按大小写敏感规则完成；精确名称读取时则沿用 SDK Getter 的查找行为。
   */
  private Collection<String> headerNames(REQUEST request) {
    if (headers == null) {
      return emptyList();
    }
    if (!enumerateHeaderNames) {
      // 全是精确名称时直接复用构造好的列表，省去每次请求的集合分配和消息头枚举。
      return exactHeaderNames;
    }
    // 配置精确名称与枚举结果可能重合，用 Set 避免同一名称重复读取。
    // 这是本次调用自己的集合，不会修改共享的 exactHeaderNames。
    Set<String> names = new LinkedHashSet<>(exactHeaderNames);
    for (String name : getter.getMessageHeaderNames(request)) {
      // 同时应用 included 与 excluded；例如匹配 tenant-* 的名称仍可能被排除规则过滤。
      if (headers.matches(name)) {
        names.add(name);
      }
    }
    return names;
  }

  /**
   * This method is internal and is hence not for public use. Its API is unstable and can change at
   * any time.
   *
   * <p>向 Instrumenter 提供内部操作分类，用于按 SpanKey 抑制重复插桩以及识别定制器适用的操作。 例如 Receive 与 Process
   * 使用不同的键，可以分别识别接收和处理；是否抑制仍取决于实际策略和父 Context。 该键不作为遥测属性输出，也不决定 SpanKind 或 Span 名称。
   */
  @Override
  public SpanKey internalGetSpanKey() {
    switch (operationType) {
      case CREATE:
        return SpanKey.PRODUCER_CREATE;
      case SEND:
        return SpanKey.PRODUCER;
      case RECEIVE:
        return SpanKey.CONSUMER_RECEIVE;
      case PROCESS:
        return SpanKey.CONSUMER_PROCESS;
      case SETTLE:
        return SpanKey.CONSUMER_SETTLE;
    }
    throw new IllegalStateException("Can't possibly happen");
  }
}
