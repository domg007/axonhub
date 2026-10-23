const prefix = '【新增策略：模型固定渠道】';

// Only for the new strategy's management panel. Never relabel provider errors
// or change the shared API client. Unknown/raw transport details are not echoed
// here because they may contain URLs, credentials or unrelated English errors.
export function modelStickyPanelError(error: Error): string {
  if (error.message.startsWith(prefix)) return error.message;
  const status = 'status' in error ? error.status : undefined;
  if (status === 401) return `${prefix}管理面板登录已失效，请重新登录后重试。`;
  if (status === 403) return `${prefix}没有访问此管理面板的权限，请检查登录账号。`;
  return `${prefix}管理面板请求失败，请检查网络和登录状态后刷新重试；这不是上游模型的测试结果。`;
}
