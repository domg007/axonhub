import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import ts from 'typescript';

const source = readFileSync(new URL('./model-sticky-error.ts', import.meta.url), 'utf8');
const { outputText } = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS } });
const exports = {};
new Function('exports', outputText)(exports);
const { modelStickyPanelError } = exports;
const prefix = '【新增策略：模型固定渠道】';

test('preserves Chinese strategy response without a repeated prefix', () => {
  const message = `${prefix}固定状态已不存在，请刷新列表后重试。`;
  assert.equal(modelStickyPanelError(new Error(message)), message);
});

test('authentication and permission errors are explained in Chinese', () => {
  for (const [status, expected] of [
    [401, '重新登录'],
    [403, '权限'],
  ]) {
    const error = Object.assign(new Error('Unauthorized'), { status });
    const message = modelStickyPanelError(error);
    assert.ok(message.startsWith(prefix));
    assert.ok(message.includes(expected));
    assert.ok(!message.includes('Unauthorized'));
  }
});

test('network errors do not expose raw credentials or look like provider test failures', () => {
  const message = modelStickyPanelError(new Error('fetch failed https://secret@example.invalid/?api_key=private'));
  assert.ok(message.startsWith(prefix));
  assert.ok(message.includes('不是上游模型的测试结果'));
  assert.ok(!message.includes('secret'));
  assert.ok(!message.includes('private'));
});

test('both management errors use the scoped helper instead of raw error messages', () => {
  const component = readFileSync(new URL('./model-sticky-controls.tsx', import.meta.url), 'utf8');
  assert.ok(component.includes('modelStickyPanelError(mutation.error)'));
  assert.ok(component.includes('modelStickyPanelError(query.error)'));
});
