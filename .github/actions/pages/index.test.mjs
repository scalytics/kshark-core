import { test } from 'node:test';
import assert from 'node:assert/strict';
import { deploy } from './index.mjs';

test('uses the uploaded artifact and waits for successful publication', async () => {
  const calls = [];
  const api = async (...args) => {
    calls.push(args);
    return calls.length === 1 ? { id: 12, page_url: 'https://example.github.io/site/' } : { status: 'succeed' };
  };
  assert.equal(await deploy(api, 42, 'oidc', 'sha', { sleep: async () => {} }), 'https://example.github.io/site/');
  assert.deepEqual(calls[0], ['POST', '/pages/deployments', { artifact_id: 42, pages_build_version: 'sha', oidc_token: 'oidc' }]);
  assert.equal(calls[1][1], '/pages/deployments/12');
});

for (const status of ['deployment_failed', 'deployment_content_failed', 'deployment_cancelled', 'deployment_lost', 'queued']) {
  test(`fails and cancels on ${status === 'queued' ? 'timeout' : status}`, async () => {
    const calls = [];
    const api = async (method, endpoint) => {
      calls.push(endpoint);
      return endpoint === '/pages/deployments' ? { id: 12 } : { status };
    };
    await assert.rejects(deploy(api, 42, 'oidc', 'sha', { sleep: async () => {}, attempts: 1 }), /ended with|timed out/);
    assert.equal(calls.at(-1), '/pages/deployments/12/cancel');
  });
}
