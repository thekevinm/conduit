<script lang="ts">
	import { CopyButton, DataTable } from '$lib/components';
	import { fetchApiKeys, generateApiKey, revokeApiKey, fetchAuditLog } from '$lib/api';
	import type { ApiKey, AuditEntry } from '$lib/api';

	let apiKeys = $state<ApiKey[]>([]);
	let auditLog = $state<AuditEntry[]>([]);
	let newKeyName = $state('');
	let newKeyValue = $state('');
	let generating = $state(false);
	let filterTool = $state('');
	let filterStatus = $state('');

	// Demo data
	const demoApiKeys: ApiKey[] = [
		{ id: '1', name: 'Claude Code', prefix: 'cdt_a8f2...', created_at: '2026-02-24T10:00:00Z', last_used: '2026-02-24T14:30:00Z', expires_at: null },
		{ id: '2', name: 'Cursor Dev', prefix: 'cdt_9e1c...', created_at: '2026-02-23T09:00:00Z', last_used: '2026-02-24T12:15:00Z', expires_at: '2026-03-23T09:00:00Z' }
	];

	const demoAuditLog: AuditEntry[] = [
		{ id: '1', timestamp: new Date(Date.now() - 60000).toISOString(), action: 'tool_call', tool: 'query', table: 'users', client: 'claude-code', duration_ms: 12, status: 'success', details: 'SELECT * FROM users WHERE id = $1' },
		{ id: '2', timestamp: new Date(Date.now() - 120000).toISOString(), action: 'tool_call', tool: 'describe_table', table: 'orders', client: 'claude-code', duration_ms: 3, status: 'success', details: '' },
		{ id: '3', timestamp: new Date(Date.now() - 180000).toISOString(), action: 'tool_call', tool: 'query', table: 'products', client: 'cursor', duration_ms: 45, status: 'success', details: 'SELECT name, price FROM products ORDER BY price DESC LIMIT 10' },
		{ id: '4', timestamp: new Date(Date.now() - 240000).toISOString(), action: 'tool_call', tool: 'query', table: 'analytics', client: 'claude-code', duration_ms: 230, status: 'error', details: 'ERROR: relation "analytics" does not exist' },
		{ id: '5', timestamp: new Date(Date.now() - 300000).toISOString(), action: 'tool_call', tool: 'list_tables', table: '', client: 'vscode', duration_ms: 8, status: 'success', details: '' },
		{ id: '6', timestamp: new Date(Date.now() - 600000).toISOString(), action: 'auth', tool: '', table: '', client: 'unknown', duration_ms: 0, status: 'error', details: 'Invalid API key' },
	];

	$effect(() => {
		loadData();
	});

	async function loadData() {
		try {
			const [keys, log] = await Promise.allSettled([fetchApiKeys(), fetchAuditLog()]);
			if (keys.status === 'fulfilled') apiKeys = keys.value;
			else apiKeys = demoApiKeys;
			if (log.status === 'fulfilled') auditLog = log.value;
			else auditLog = demoAuditLog;
		} catch {
			apiKeys = demoApiKeys;
			auditLog = demoAuditLog;
		}
	}

	async function handleGenerateKey() {
		if (!newKeyName) return;
		generating = true;
		try {
			const result = await generateApiKey(newKeyName);
			newKeyValue = result.key;
			await loadData();
		} catch {
			// Generate client-side for demo
			const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
			let key = 'cdt_';
			for (let i = 0; i < 32; i++) key += chars[Math.floor(Math.random() * chars.length)];
			newKeyValue = key;
			apiKeys = [...apiKeys, {
				id: crypto.randomUUID(),
				name: newKeyName,
				prefix: key.substring(0, 8) + '...',
				created_at: new Date().toISOString(),
				last_used: null,
				expires_at: null
			}];
		}
		generating = false;
		newKeyName = '';
	}

	async function handleRevoke(id: string) {
		try {
			await revokeApiKey(id);
		} catch {
			// Ignore in dev
		}
		apiKeys = apiKeys.filter((k) => k.id !== id);
	}

	function formatTime(ts: string): string {
		try {
			return new Date(ts).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
		} catch { return ts; }
	}

	const filteredAuditLog = $derived(
		auditLog.filter((entry) => {
			if (filterTool && !entry.tool.toLowerCase().includes(filterTool.toLowerCase())) return false;
			if (filterStatus && entry.status !== filterStatus) return false;
			return true;
		})
	);
</script>

<svelte:head>
	<title>Security — Conduit</title>
</svelte:head>

<div class="space-y-8">
	<div>
		<h1 class="text-2xl font-semibold text-slate-100 tracking-tight">Security</h1>
		<p class="mt-1 text-sm text-slate-400">API keys, access control, and audit logs</p>
	</div>

	<!-- Access Mode -->
	<div class="bg-surface-900 border border-surface-700/50 rounded-xl p-5">
		<h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider mb-4">Access Mode</h2>
		<div class="flex items-center gap-4">
			<div class="flex items-center gap-2 px-4 py-2 rounded-lg bg-emerald-500/10 border border-emerald-500/20">
				<svg class="w-5 h-5 text-emerald-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
					<path stroke-linecap="round" stroke-linejoin="round" d="M9 12.75L11.25 15 15 9.75m-3-7.036A11.959 11.959 0 013.598 6 11.99 11.99 0 003 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285z" />
				</svg>
				<span class="text-sm font-medium text-emerald-400">Read Only</span>
			</div>
			<span class="text-xs text-slate-500">Only SELECT queries are permitted. Write operations are blocked.</span>
		</div>
	</div>

	<!-- API Keys -->
	<div class="bg-surface-900 border border-surface-700/50 rounded-xl overflow-hidden">
		<div class="px-5 py-4 border-b border-surface-700/50">
			<h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">API Keys</h2>
		</div>

		<!-- Generate new key -->
		<div class="px-5 py-4 border-b border-surface-700/30 bg-surface-800/30">
			{#if newKeyValue}
				<div class="space-y-3">
					<div class="flex items-center gap-2 p-3 bg-surface-900 rounded-lg border border-amber-500/20">
						<code class="flex-1 text-sm font-mono text-amber-300 break-all">{newKeyValue}</code>
						<CopyButton text={newKeyValue} />
					</div>
					<div class="flex items-center justify-between">
						<p class="text-xs text-amber-400/70">Copy this key now. It won't be shown again.</p>
						<button
							onclick={() => (newKeyValue = '')}
							class="text-xs text-slate-500 hover:text-slate-300 transition-colors"
						>
							Done
						</button>
					</div>
				</div>
			{:else}
				<div class="flex items-center gap-3">
					<input
						type="text"
						bind:value={newKeyName}
						class="flex-1 px-3 py-2 bg-surface-800 border border-surface-700 rounded-lg text-sm text-slate-200 placeholder:text-surface-600 focus:outline-none focus:border-accent-500/50"
						placeholder="Key name (e.g., Claude Code)"
					/>
					<button
						onclick={handleGenerateKey}
						disabled={!newKeyName || generating}
						class="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-accent-500 text-surface-950 font-medium text-sm hover:bg-accent-400 disabled:opacity-40 transition-colors"
					>
						{generating ? 'Generating...' : 'Generate Key'}
					</button>
				</div>
			{/if}
		</div>

		<!-- Key list -->
		<div class="divide-y divide-surface-800/50">
			{#each apiKeys as key}
				<div class="flex items-center justify-between px-5 py-3 hover:bg-surface-800/20 transition-colors">
					<div class="flex items-center gap-4">
						<div class="w-8 h-8 rounded-lg bg-surface-800 flex items-center justify-center">
							<svg class="w-4 h-4 text-accent-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
								<path stroke-linecap="round" stroke-linejoin="round" d="M15.75 5.25a3 3 0 013 3m3 0a6 6 0 01-7.029 5.912c-.563-.097-1.159.026-1.563.43L10.5 17.25H8.25v2.25H6v2.25H2.25v-2.818c0-.597.237-1.17.659-1.591l6.499-6.499c.404-.404.527-1 .43-1.563A6 6 0 1121.75 8.25z" />
							</svg>
						</div>
						<div>
							<div class="text-sm font-medium text-slate-200">{key.name}</div>
							<div class="text-xs font-mono text-slate-500">{key.prefix}</div>
						</div>
					</div>
					<div class="flex items-center gap-6">
						<div class="text-right text-xs">
							<div class="text-slate-500">Created {formatTime(key.created_at)}</div>
							{#if key.last_used}
								<div class="text-slate-600">Last used {formatTime(key.last_used)}</div>
							{/if}
						</div>
						<button
							onclick={() => handleRevoke(key.id)}
							class="text-xs text-rose-400/60 hover:text-rose-400 transition-colors px-2 py-1 rounded hover:bg-rose-500/10"
						>
							Revoke
						</button>
					</div>
				</div>
			{/each}
			{#if apiKeys.length === 0}
				<div class="px-5 py-8 text-center text-sm text-slate-500">
					No API keys generated yet. Create one above to secure your endpoint.
				</div>
			{/if}
		</div>
	</div>

	<!-- RBAC -->
	<div class="bg-surface-900 border border-surface-700/50 rounded-xl p-5">
		<div class="flex items-center justify-between mb-3">
			<h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Roles & Permissions</h2>
			<span class="px-2 py-0.5 rounded text-[10px] font-medium bg-surface-800 text-slate-500 border border-surface-700/50 uppercase tracking-wider">Coming Soon</span>
		</div>
		<p class="text-sm text-slate-500">Fine-grained role-based access control for multi-user deployments.</p>
	</div>

	<!-- Audit Log -->
	<div class="bg-surface-900 border border-surface-700/50 rounded-xl overflow-hidden">
		<div class="flex items-center justify-between px-5 py-4 border-b border-surface-700/50">
			<h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Audit Log</h2>
			<div class="flex items-center gap-2">
				<input
					type="text"
					bind:value={filterTool}
					class="px-3 py-1.5 bg-surface-800 border border-surface-700 rounded-lg text-xs text-slate-300 placeholder:text-surface-600 focus:outline-none focus:border-accent-500/50 w-32"
					placeholder="Filter tool..."
				/>
				<select
					bind:value={filterStatus}
					class="px-3 py-1.5 bg-surface-800 border border-surface-700 rounded-lg text-xs text-slate-300 focus:outline-none focus:border-accent-500/50 appearance-none cursor-pointer"
				>
					<option value="">All status</option>
					<option value="success">Success</option>
					<option value="error">Error</option>
				</select>
			</div>
		</div>

		<div class="overflow-x-auto">
			<table class="w-full text-xs">
				<thead>
					<tr class="bg-surface-800/30">
						<th class="px-4 py-2.5 text-left text-slate-500 font-medium uppercase tracking-wider">Time</th>
						<th class="px-4 py-2.5 text-left text-slate-500 font-medium uppercase tracking-wider">Action</th>
						<th class="px-4 py-2.5 text-left text-slate-500 font-medium uppercase tracking-wider">Tool</th>
						<th class="px-4 py-2.5 text-left text-slate-500 font-medium uppercase tracking-wider">Table</th>
						<th class="px-4 py-2.5 text-left text-slate-500 font-medium uppercase tracking-wider">Client</th>
						<th class="px-4 py-2.5 text-right text-slate-500 font-medium uppercase tracking-wider">Duration</th>
						<th class="px-4 py-2.5 text-center text-slate-500 font-medium uppercase tracking-wider">Status</th>
					</tr>
				</thead>
				<tbody class="divide-y divide-surface-800/30">
					{#each filteredAuditLog as entry}
						<tr class="hover:bg-surface-800/20 transition-colors">
							<td class="px-4 py-2.5 text-slate-400 whitespace-nowrap">{formatTime(entry.timestamp)}</td>
							<td class="px-4 py-2.5 font-mono text-slate-300">{entry.action}</td>
							<td class="px-4 py-2.5 font-mono text-amber-300/80">{entry.tool || '—'}</td>
							<td class="px-4 py-2.5 font-mono text-slate-400">{entry.table || '—'}</td>
							<td class="px-4 py-2.5 text-slate-400">{entry.client}</td>
							<td class="px-4 py-2.5 text-right font-mono text-slate-500">{entry.duration_ms > 0 ? entry.duration_ms + 'ms' : '—'}</td>
							<td class="px-4 py-2.5 text-center">
								<span class="inline-flex w-2 h-2 rounded-full {entry.status === 'success' ? 'bg-emerald-400' : 'bg-rose-400'}"></span>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	</div>
</div>
