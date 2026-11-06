// 二开（05/02/04）新增的 Webhook 事件清单。
//
// beta12 起上游在 webhook-settings.tsx 里自带「按事件勾选订阅」和一个事件数组
// （channel.auto_disabled + 三个 provider.quota.*）。我们不再改写上游的勾选逻辑，
// 只在上游数组里用一行 `...FORK_WEBHOOK_EVENTS` 把下面三个事件插进去，
// 这样那个上游文件只多出两行（这一行和 import），以后合并几乎不会冲突。
//
// ⚠️ id 必须与后端 internal/server/biz 里的 Go 常量字面量完全一致，
// 拼错不会报错，只会表现为「勾了但收不到通知」：
//   - channel_model_fetch_alert.go   EventChannelModelFetchFailed / EventChannelModelFetchRecovered
//   - channel_model_change_alert.go  EventChannelModelsChanged
// 对应的说明文字在 locales/{en,zh-CN}/fork.json。
//
// 字段名 id / i18nKey 与上游数组元素保持一致，`as const` 让类型和上游条目同形。
export const FORK_WEBHOOK_EVENTS = [
  { id: 'channel.model_fetch_failed', i18nKey: 'system.webhook.events.channelModelFetchFailed' },
  { id: 'channel.model_fetch_recovered', i18nKey: 'system.webhook.events.channelModelFetchRecovered' },
  { id: 'channel.models_changed', i18nKey: 'system.webhook.events.channelModelsChanged' },
] as const;
