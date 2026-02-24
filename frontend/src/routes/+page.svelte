<script lang="ts">
	import { base } from '$app/paths';
	import { StatsCard, CopyButton, StatusPill } from '$lib/components';
	import { app } from '$lib/stores/app.svelte';

	const stats = $derived(app.stats);
	const activity = $derived(app.activity);
	const mcpEndpoint = $derived('http://localhost:8080/mcp');

	function formatDuration(ms: number): string {
		if (ms < 1) return '<1ms';
		if (ms < 1000) return `${Math.round(ms)}ms`;
		return `${(ms / 1000).toFixed(1)}s`;
	}

	function formatTime(ts: string): string {
		try {
			const d = new Date(ts);
			return d.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
		} catch {
			return ts;
		}
	}

	// Demo activity data for when API isn't available
	const demoActivity = [
		{ id: '1', tool: 'query', table: 'users', duration_ms: 12, timestamp: new Date(Date.now() - 30000).toISOString(), status: 'success' as const, client: 'claude-code' },
		{ id: '2', tool: 'describe_table', table: 'orders', duration_ms: 3, timestamp: new Date(Date.now() - 45000).toISOString(), status: 'success' as const, client: 'claude-code' },
		{ id: '3', tool: 'list_tables', table: '—', duration_ms: 8, timestamp: new Date(Date.now() - 60000).toISOString(), status: 'success' as const, client: 'cursor' },
		{ id: '4', tool: 'query', table: 'products', duration_ms: 45, timestamp: new Date(Date.now() - 90000).toISOString(), status: 'success' as const, client: 'claude-code' },
		{ id: '5', tool: 'get_users_by_id', table: 'users', duration_ms: 5, timestamp: new Date(Date.now() - 120000).toISOString(), status: 'success' as const, client: 'vscode' },
		{ id: '6', tool: 'query', table: 'analytics', duration_ms: 230, timestamp: new Date(Date.now() - 180000).toISOString(), status: 'error' as const, client: 'claude-code' },
	];

	const displayActivity = $derived(activity.length > 0 ? activity : demoActivity);
</script>

<svelte:head>
	<title>Dashboard — Conduit</title>
</svelte:head>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div>
			<h1 class="text-2xl font-semibold text-slate-100 tracking-tight">Dashboard</h1>
			<p class="mt-1 text-sm text-slate-400">Your MCP server at a glance</p>
		</div>
		<div class="flex items-center gap-3">
			<a
				href="{base}/setup"
				class="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-accent-500 text-surface-950 font-medium text-sm hover:bg-accent-400 transition-colors"
			>
				<svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
					<path stroke-linecap="round" stroke-linejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
				</svg>
				Add Source
			</a>
		</div>
	</div>

	<!-- Stats Grid -->
	<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
		<StatsCard label="Sources Connected" value={stats?.sources_connected ?? 0}>
			{#snippet icon()}
				<svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
					<path stroke-linecap="round" stroke-linejoin="round" d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375" />
				</svg>
			{/snippet}
		</StatsCard>

		<StatsCard label="Tables Exposed" value={stats?.tables_exposed ?? 0}>
			{#snippet icon()}
				<svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
					<path stroke-linecap="round" stroke-linejoin="round" d="M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 01-1.125-1.125M3.375 19.5h7.5c.621 0 1.125-.504 1.125-1.125m-9.75 0V5.625m0 12.75v-1.5c0-.621.504-1.125 1.125-1.125m18.375 2.625V5.625m0 12.75c0 .621-.504 1.125-1.125 1.125m1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125m0 3.75h-7.5A1.125 1.125 0 0112 18.375m9.75-12.75c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125m19.5 0v1.5c0 .621-.504 1.125-1.125 1.125M2.25 5.625v1.5c0 .621.504 1.125 1.125 1.125m0 0h17.25m-17.25 0h7.5c.621 0 1.125.504 1.125 1.125M3.375 8.25c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125m17.25-3.75h-7.5c-.621 0-1.125.504-1.125 1.125m8.625-1.125c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125M12 10.875v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 10.875c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125M12 12h7.5m-7.5 0c-.621 0-1.125.504-1.125 1.125M20.625 12c.621 0 1.125.504 1.125 1.125v1.5c0 .621-.504 1.125-1.125 1.125m-17.25 0h7.5M12 14.625v-1.5m0 1.5c0 .621-.504 1.125-1.125 1.125M12 14.625c0 .621.504 1.125 1.125 1.125m-2.25 0c.621 0 1.125.504 1.125 1.125m0 0v1.5c0 .621-.504 1.125-1.125 1.125" />
				</svg>
			{/snippet}
		</StatsCard>

		<StatsCard label="Queries Today" value={stats?.queries_today ?? 0}>
			{#snippet icon()}
				<svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
					<path stroke-linecap="round" stroke-linejoin="round" d="M3.75 13.5l10.5-11.25L12 10.5h8.25L9.75 21.75 12 13.5H3.75z" />
				</svg>
			{/snippet}
		</StatsCard>

		<StatsCard label="Avg Latency" value={stats?.avg_latency_ms ?? 0} suffix="ms">
			{#snippet icon()}
				<svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
					<path stroke-linecap="round" stroke-linejoin="round" d="M12 6v6h4.5m4.5 0a9 9 0 11-18 0 9 9 0 0118 0z" />
				</svg>
			{/snippet}
		</StatsCard>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- Activity Feed -->
		<div class="lg:col-span-2 bg-surface-900 border border-surface-700/50 rounded-xl overflow-hidden">
			<div class="flex items-center justify-between px-5 py-4 border-b border-surface-700/50">
				<h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Recent Activity</h2>
				<span class="relative flex h-2 w-2">
					<span class="absolute inset-0 rounded-full bg-emerald-400 opacity-40 animate-ping"></span>
					<span class="relative inline-flex h-2 w-2 rounded-full bg-emerald-400"></span>
				</span>
			</div>
			<div class="divide-y divide-surface-800/50 max-h-[400px] overflow-y-auto">
				{#each displayActivity as entry}
					<div class="flex items-center gap-4 px-5 py-3 hover:bg-surface-800/30 transition-colors">
						<div class="flex-shrink-0 w-1.5 h-1.5 rounded-full {entry.status === 'success' ? 'bg-emerald-400' : 'bg-rose-400'}"></div>
						<div class="flex-1 min-w-0">
							<div class="flex items-center gap-2">
								<span class="font-mono text-sm text-amber-300/90 truncate">{entry.tool}</span>
								{#if entry.table && entry.table !== '—'}
									<span class="text-surface-600">on</span>
									<span class="font-mono text-sm text-slate-300 truncate">{entry.table}</span>
								{/if}
							</div>
						</div>
						<span class="flex-shrink-0 text-xs font-mono text-surface-600">{formatDuration(entry.duration_ms)}</span>
						<span class="flex-shrink-0 text-xs text-slate-500">{formatTime(entry.timestamp)}</span>
						<span class="flex-shrink-0 px-2 py-0.5 rounded text-xs font-medium bg-surface-800 text-slate-500">{entry.client}</span>
					</div>
				{/each}
			</div>
		</div>

		<!-- Quick Info Panel -->
		<div class="space-y-4">
			<!-- MCP Endpoint -->
			<div class="bg-surface-900 border border-surface-700/50 rounded-xl p-5">
				<h3 class="text-sm font-semibold text-slate-200 uppercase tracking-wider mb-3">MCP Endpoint</h3>
				<div class="flex items-center gap-2 p-3 bg-surface-800/80 rounded-lg border border-surface-700/30">
					<code class="flex-1 text-sm font-mono text-amber-300/90 truncate">{mcpEndpoint}</code>
					<CopyButton text={mcpEndpoint} label="" />
				</div>
				<div class="mt-3 flex items-center gap-2">
					<span class="text-xs text-slate-500">Transport:</span>
					<span class="px-2 py-0.5 rounded text-xs font-mono font-medium bg-surface-800 text-slate-400 border border-surface-700/50">HTTP</span>
				</div>
			</div>

			<!-- Quick Actions -->
			<div class="bg-surface-900 border border-surface-700/50 rounded-xl p-5">
				<h3 class="text-sm font-semibold text-slate-200 uppercase tracking-wider mb-3">Quick Actions</h3>
				<div class="space-y-2">
					<a
						href="{base}/setup"
						class="flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm text-slate-300 hover:bg-surface-800/50 hover:text-slate-100 transition-colors border border-transparent hover:border-surface-700/50"
					>
						<svg class="w-4 h-4 text-accent-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
							<path stroke-linecap="round" stroke-linejoin="round" d="M12 4.5v15m7.5-7.5h-15" />
						</svg>
						Add Source
					</a>
					<a
						href="{base}/sources"
						class="flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm text-slate-300 hover:bg-surface-800/50 hover:text-slate-100 transition-colors border border-transparent hover:border-surface-700/50"
					>
						<svg class="w-4 h-4 text-accent-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
							<path stroke-linecap="round" stroke-linejoin="round" d="M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 01-1.125-1.125M3.375 19.5h7.5c.621 0 1.125-.504 1.125-1.125" />
						</svg>
						View Schema
					</a>
					<a
						href="{base}/endpoints"
						class="flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm text-slate-300 hover:bg-surface-800/50 hover:text-slate-100 transition-colors border border-transparent hover:border-surface-700/50"
					>
						<svg class="w-4 h-4 text-accent-500" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
							<path stroke-linecap="round" stroke-linejoin="round" d="M9.594 3.94c.09-.542.56-.94 1.11-.94h2.593c.55 0 1.02.398 1.11.94l.213 1.281c.063.374.313.686.645.87.074.04.147.083.22.127.324.196.72.257 1.075.124l1.217-.456a1.125 1.125 0 011.37.49l1.296 2.247a1.125 1.125 0 01-.26 1.431l-1.003.827c-.293.24-.438.613-.431.992a6.759 6.759 0 010 .255c-.007.378.138.75.43.99l1.005.828c.424.35.534.954.26 1.43l-1.298 2.247a1.125 1.125 0 01-1.369.491l-1.217-.456c-.355-.133-.75-.072-1.076.124a6.57 6.57 0 01-.22.128c-.331.183-.581.495-.644.869l-.213 1.28c-.09.543-.56.941-1.11.941h-2.594c-.55 0-1.02-.398-1.11-.94l-.213-1.281c-.062-.374-.312-.686-.644-.87a6.52 6.52 0 01-.22-.127c-.325-.196-.72-.257-1.076-.124l-1.217.456a1.125 1.125 0 01-1.369-.49l-1.297-2.247a1.125 1.125 0 01.26-1.431l1.004-.827c.292-.24.437-.613.43-.992a6.932 6.932 0 010-.255c.007-.378-.138-.75-.43-.99l-1.004-.828a1.125 1.125 0 01-.26-1.43l1.297-2.247a1.125 1.125 0 011.37-.491l1.216.456c.356.133.751.072 1.076-.124.072-.044.146-.087.22-.128.332-.183.582-.495.644-.869l.214-1.281z" />
						</svg>
						Open Config
					</a>
				</div>
			</div>

			<!-- Server Info -->
			<div class="bg-surface-900 border border-surface-700/50 rounded-xl p-5">
				<h3 class="text-sm font-semibold text-slate-200 uppercase tracking-wider mb-3">Server</h3>
				<dl class="space-y-2 text-sm">
					<div class="flex justify-between">
						<dt class="text-slate-500">Uptime</dt>
						<dd class="font-mono text-slate-300">{app.health ? Math.floor(app.health.uptime / 60) + 'm' : '—'}</dd>
					</div>
					<div class="flex justify-between">
						<dt class="text-slate-500">Sources</dt>
						<dd class="font-mono text-slate-300">{app.sourceCount}</dd>
					</div>
					<div class="flex justify-between">
						<dt class="text-slate-500">Mode</dt>
						<dd class="font-mono text-slate-300">{app.sources[0]?.access_mode ?? 'read-only'}</dd>
					</div>
				</dl>
			</div>
		</div>
	</div>
</div>
