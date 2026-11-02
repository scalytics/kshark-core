import { DefaultArtifactClient } from '@actions/artifact';
import * as core from '@actions/core';
import { pathToFileURL } from 'node:url';
import path from 'node:path';

export async function deploy(api, artifactId, oidcToken, revision, {
  sleep = ms => new Promise(resolve => setTimeout(resolve, ms)),
  attempts = 120,
} = {}) {
  const deployment = await api('POST', '/pages/deployments', {
    artifact_id: artifactId,
    pages_build_version: revision,
    oidc_token: oidcToken,
  });
  const id = deployment.id ?? deployment.status_url?.split('/').pop();
  if (!id) throw new Error('Pages returned no deployment ID');
  try {
    for (let i = 0; i < attempts; i++) {
      await sleep(5000);
      const { status } = await api('GET', `/pages/deployments/${encodeURIComponent(id)}`);
      if (status === 'succeed') return deployment.page_url;
      if (['deployment_failed', 'deployment_content_failed', 'deployment_cancelled', 'deployment_lost'].includes(status)) {
        throw new Error(`Pages deployment ended with ${status}`);
      }
    }
    throw new Error('Pages deployment timed out after 10 minutes');
  } catch (error) {
    await api('POST', `/pages/deployments/${encodeURIComponent(id)}/cancel`).catch(() => {});
    throw error;
  }
}

async function main() {
  const mode = core.getInput('mode', { required: true });
  if (mode === 'upload') {
    const archive = path.join(process.env.RUNNER_TEMP, 'artifact.tar');
    const { id } = await new DefaultArtifactClient().uploadArtifact(
      'github-pages', [archive], process.env.RUNNER_TEMP, { retentionDays: 1 },
    );
    if (!id) throw new Error('Pages artifact upload returned no ID');
    core.setOutput('artifact-id', id);
    return;
  }
  if (mode !== 'deploy') throw new Error(`Unknown Pages mode: ${mode}`);
  const token = core.getInput('token', { required: true });
  core.setSecret(token);
  const artifactId = Number(core.getInput('artifact-id', { required: true }));
  if (!Number.isSafeInteger(artifactId) || artifactId <= 0) throw new Error('Invalid artifact ID');
  const api = async (method, endpoint, body) => {
    const response = await fetch(`${process.env.GITHUB_API_URL}/repos/${process.env.GITHUB_REPOSITORY}${endpoint}`, {
      method,
      headers: { Authorization: `Bearer ${token}`, Accept: 'application/vnd.github+json', 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: AbortSignal.timeout(30000),
    });
    if (!response.ok) throw new Error(`Pages API ${method} ${endpoint} failed: HTTP ${response.status}`);
    return response.status === 204 ? {} : response.json();
  };
  const url = await deploy(api, artifactId, await core.getIDToken(), process.env.GITHUB_SHA);
  core.setOutput('page_url', url);
  core.info(`Published ${url}`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch(error => core.setFailed(error.message));
}
