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
	let piiMaskingEnabled = $state(true);

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

	<!-- PII Masking -->
	<div class="bg-surface-900 border border-surface-700/50 rounded-xl overflow-hidden">
		<div class="flex items-center justify-between px-5 py-4 border-b border-surface-700/50">
			<h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">PII Masking</h2>
			<button
				onclick={() => (piiMaskingEnabled = !piiMaskingEnabled)}
				class="relative inline-flex h-6 w-11 items-center rounded-full transition-colors {piiMaskingEnabled ? 'bg-accent-500' : 'bg-surface-700'}"
				aria-label="Toggle PII masking"
			>
				<span
					class="inline-block h-4 w-4 rounded-full bg-white transition-transform {piiMaskingEnabled ? 'translate-x-6' : 'translate-x-1'}"
				></span>
			</button>
		</div>

		<div class="px-5 py-4 border-b border-surface-700/30 bg-surface-800/30">
			<div class="flex items-center gap-3">
				<svg class="w-5 h-5 text-accent-400 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
					<path stroke-linecap="round" stroke-linejoin="round" d="M3.98 8.223A10.477 10.477 0 001.934 12C3.226 16.338 7.244 19.5 12 19.5c.993 0 1.953-.138 2.863-.395M6.228 6.228A10.45 10.45 0 0112 4.5c4.756 0 8.773 3.162 10.065 7.498a10.523 10.523 0 01-4.293 5.774M6.228 6.228L3 3m3.228 3.228l3.65 3.65m7.894 7.894L21 21m-3.228-3.228l-3.65-3.65m0 0a3 3 0 10-4.243-4.243m4.242 4.242L9.88 9.88" />
				</svg>
				<div>
					<p class="text-sm text-slate-300">
						{#if piiMaskingEnabled}
							PII masking is <span class="text-accent-500 font-medium">enabled</span>. Sensitive columns are automatically detected and masked in query results.
						{:else}
							PII masking is <span class="text-rose-400 font-medium">disabled</span>. All column values are returned unmasked.
						{/if}
					</p>
					<p class="text-xs text-slate-500 mt-1">Equivalent to the <code class="text-accent-500/80 bg-surface-800 px-1.5 py-0.5 rounded font-mono text-[11px]">--mask-pii</code> flag</p>
				</div>
			</div>
		</div>

		<div class="overflow-x-auto">
			<table class="w-full text-xs">
				<thead>
					<tr class="bg-surface-800/30">
						<th class="px-4 py-2.5 text-left text-slate-500 font-medium uppercase tracking-wider">Column Pattern</th>
						<th class="px-4 py-2.5 text-left text-slate-500 font-medium uppercase tracking-wider">Matches</th>
						<th class="px-4 py-2.5 text-left text-slate-500 font-medium uppercase tracking-wider">Action</th>
						<th class="px-4 py-2.5 text-left text-slate-500 font-medium uppercase tracking-wider">Example Output</th>
					</tr>
				</thead>
				<tbody class="divide-y divide-surface-800/30">
					<tr class="hover:bg-surface-800/20 transition-colors">
						<td class="px-4 py-2.5 font-mono text-accent-500">*email*</td>
						<td class="px-4 py-2.5 text-slate-400">Email addresses</td>
						<td class="px-4 py-2.5"><span class="px-2 py-0.5 rounded text-[10px] font-medium bg-amber-500/10 text-amber-400 border border-amber-500/20 uppercase tracking-wider">Masked</span></td>
						<td class="px-4 py-2.5 font-mono text-slate-300">k***@example.com</td>
					</tr>
					<tr class="hover:bg-surface-800/20 transition-colors">
						<td class="px-4 py-2.5 font-mono text-accent-500">*phone*, *mobile*</td>
						<td class="px-4 py-2.5 text-slate-400">Phone numbers</td>
						<td class="px-4 py-2.5"><span class="px-2 py-0.5 rounded text-[10px] font-medium bg-amber-500/10 text-amber-400 border border-amber-500/20 uppercase tracking-wider">Masked</span></td>
						<td class="px-4 py-2.5 font-mono text-slate-300">***-***-1234</td>
					</tr>
					<tr class="hover:bg-surface-800/20 transition-colors">
						<td class="px-4 py-2.5 font-mono text-accent-500">*ssn*</td>
						<td class="px-4 py-2.5 text-slate-400">Social Security numbers</td>
						<td class="px-4 py-2.5"><span class="px-2 py-0.5 rounded text-[10px] font-medium bg-amber-500/10 text-amber-400 border border-amber-500/20 uppercase tracking-wider">Masked</span></td>
						<td class="px-4 py-2.5 font-mono text-slate-300">***-**-6789</td>
					</tr>
					<tr class="hover:bg-surface-800/20 transition-colors">
						<td class="px-4 py-2.5 font-mono text-accent-500">*card_number*</td>
						<td class="px-4 py-2.5 text-slate-400">Credit card numbers</td>
						<td class="px-4 py-2.5"><span class="px-2 py-0.5 rounded text-[10px] font-medium bg-amber-500/10 text-amber-400 border border-amber-500/20 uppercase tracking-wider">Masked</span></td>
						<td class="px-4 py-2.5 font-mono text-slate-300">****-****-****-1234</td>
					</tr>
					<tr class="hover:bg-surface-800/20 transition-colors">
						<td class="px-4 py-2.5 font-mono text-accent-500">*password*, *secret*</td>
						<td class="px-4 py-2.5 text-slate-400">Passwords and secrets</td>
						<td class="px-4 py-2.5"><span class="px-2 py-0.5 rounded text-[10px] font-medium bg-rose-500/10 text-rose-400 border border-rose-500/20 uppercase tracking-wider">Excluded</span></td>
						<td class="px-4 py-2.5 text-slate-500 italic">Column hidden entirely</td>
					</tr>
					<tr class="hover:bg-surface-800/20 transition-colors">
						<td class="px-4 py-2.5 font-mono text-accent-500">*token*, *api_key*</td>
						<td class="px-4 py-2.5 text-slate-400">Tokens and API keys</td>
						<td class="px-4 py-2.5"><span class="px-2 py-0.5 rounded text-[10px] font-medium bg-rose-500/10 text-rose-400 border border-rose-500/20 uppercase tracking-wider">Excluded</span></td>
						<td class="px-4 py-2.5 text-slate-500 italic">Column hidden entirely</td>
					</tr>
				</tbody>
			</table>
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
