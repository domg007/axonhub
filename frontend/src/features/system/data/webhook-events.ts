// Webhook 可订阅事件的单一数据源。
//
// 之所以单独开一个文件，而不是把数组塞进 webhook-settings.tsx：
// 那个组件是上游文件，改动越少、与上游的合并冲突就越小。
// 事件清单和勾选逻辑放在这里，组件里只剩下薄薄的调用。
//
// ⚠️ event 字段必须与后端 internal/server/biz 里的 Go 常量字面量完全一致，
// 拼错不会报错，只会表现为「勾了但收不到通知」：
//   - webhook_notifier.go            EventChannelAutoDisabled
//   - channel_model_fetch_alert.go   EventChannelModelFetchFailed / EventChannelModelFetchRecovered
//   - channel_model_change_alert.go  EventChannelModelsChanged

import type { WebhookSubscription } from './system';

export interface WebhookEventDescriptor {
  /** 后端事件名，同时用作订阅记录里的 event 字段 */
  event: string;
  /** i18n key，用于展示「这个事件什么时候触发」 */
  descriptionKey: string;
}

export const WEBHOOK_EVENTS: readonly WebhookEventDescriptor[] = [
  {
    event: 'channel.auto_disabled',
    descriptionKey: 'system.webhook.events.channelAutoDisabled',
  },
  {
    event: 'channel.model_fetch_failed',
    descriptionKey: 'system.webhook.events.channelModelFetchFailed',
  },
  {
    event: 'channel.model_fetch_recovered',
    descriptionKey: 'system.webhook.events.channelModelFetchRecovered',
  },
  {
    event: 'channel.models_changed',
    descriptionKey: 'system.webhook.events.channelModelsChanged',
  },
];

/** 返回订阅了指定事件的通知目标名集合。事件不存在时返回空集合。 */
export function getSubscribedTargetNames(subscriptions: WebhookSubscription[], event: string): Set<string> {
  return new Set(subscriptions.find((subscription) => subscription.event === event)?.targetNames || []);
}

/**
 * 勾选 / 取消某个「事件 × 通知目标」组合，返回新的订阅数组。
 *
 * 约定与原有单事件实现保持一致：
 *   - 目标名为空（用户还没填名字）时原样返回，不产生脏数据
 *   - 某事件的目标列表被清空后，整条订阅记录一并删除，而不是留一条空数组
 *   - 其它事件的订阅记录不受影响
 */
export function toggleEventSubscription(
  subscriptions: WebhookSubscription[],
  event: string,
  targetName: string,
  checked: boolean
): WebhookSubscription[] {
  const normalizedName = targetName.trim();
  if (!normalizedName) {
    return subscriptions;
  }

  const current = subscriptions.find((subscription) => subscription.event === event);
  const nextTargetNames = checked
    ? Array.from(new Set([...(current?.targetNames || []), normalizedName]))
    : (current?.targetNames || []).filter((name) => name !== normalizedName);

  const others = subscriptions.filter((subscription) => subscription.event !== event);
  if (nextTargetNames.length === 0) {
    return others;
  }

  return [...others, { event, targetNames: nextTargetNames }];
}
