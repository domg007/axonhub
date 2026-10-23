import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import ts from 'typescript';

const source = readFileSync(new URL('./model-sticky-health.ts', import.meta.url), 'utf8');
const { outputText } = ts.transpileModule(source, { compilerOptions: { module: ts.ModuleKind.CommonJS } });
const exports = {};
new Function('exports', outputText)(exports);
const { modelStickyCooldownActive } = exports;

test('distinguishes active cooldown from retained failure history at the exact boundary', () => {
  const now = Date.parse('2026-10-24T00:00:00Z');
  assert.equal(modelStickyCooldownActive(new Date(now + 1).toISOString(), now), true);
  assert.equal(modelStickyCooldownActive(new Date(now).toISOString(), now), false);
  assert.equal(modelStickyCooldownActive(new Date(now - 1).toISOString(), now), false);
});

test('panel and both locales distinguish expired cooldown and temporary manual targets', () => {
  const component = readFileSync(new URL('./model-sticky-controls.tsx', import.meta.url), 'utf8');
  assert.ok(component.includes('modelStickyCooldownActive(health.until)'));
  assert.ok(component.includes("t('system.retry.modelSticky.failureHistory')"));
  for (const locale of ['en', 'zh-CN']) {
    const data = JSON.parse(readFileSync(new URL(`../../../locales/${locale}/system.json`, import.meta.url), 'utf8'));
    assert.ok(data['system.retry.modelSticky.failureHistory']);
    assert.ok(data['system.retry.modelSticky.description']);
    assert.ok(data['system.retry.modelSticky.preferred']);
    const doc = data['system.retry.loadBalancerStrategy.documentation.model-sticky'];
    assert.ok(doc.includes(locale === 'en' ? 'configured models' : '自建模型配置'));
    assert.ok(doc.includes(locale === 'en' ? 'not channel tests' : '不用于渠道测试'));
    assert.ok(doc.includes(locale === 'en' ? 'All upstream errors, regardless of HTTP status' : '所有上游错误不区分HTTP状态码'));
    const preferred = data['system.retry.modelSticky.preferred'];
    assert.ok(preferred.includes(locale === 'en' ? 'all upstream errors' : '所有上游错误'));
    assert.ok(!preferred.includes(locale === 'en' ? 'parameter errors or cancellation do not revoke' : '参数错误或取消不会撤销'));
  }
  assert.equal(
    ts.transpileModule(component, { compilerOptions: { jsx: ts.JsxEmit.ReactJSX }, reportDiagnostics: true }).diagnostics.length,
    0
  );
});
