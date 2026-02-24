<script lang="ts">
	import '../app.css';
	import { Sidebar, TopBar } from '$lib/components';
	import { app } from '$lib/stores/app.svelte';
	import { fetchHealth, fetchSources, fetchStats, fetchActivity } from '$lib/api';
	import type { Snippet } from 'svelte';

	let { children }: { children: Snippet } = $props();

	// Poll server health and data
	async function loadData() {
		try {
			const [health, sources, stats, activity] = await Promise.allSettled([
				fetchHealth(),
				fetchSources(),
				fetchStats(),
				fetchActivity()
			]);

			if (health.status === 'fulfilled') app.health = health.value;
			if (sources.status === 'fulfilled') app.sources = sources.value;
			if (stats.status === 'fulfilled') app.stats = stats.value;
			if (activity.status === 'fulfilled') app.activity = activity.value;
		} catch {
			// API not available — use demo data for development
			app.health = { status: 'ok', version: '0.1.0-dev', uptime: 3600, sources: 1 };
			app.sources = [
				{
					id: 'demo-pg',
					name: 'Demo PostgreSQL',
					type: 'PostgreSQL',
					host: 'localhost',
					port: 5432,
					database: 'demo',
					tables: 12,
					status: 'connected',
					access_mode: 'read-only',
					created_at: new Date().toISOString()
				}
			];
			app.stats = { sources_connected: 1, tables_exposed: 12, queries_today: 47, avg_latency_ms: 23 };
			app.activity = [];
		}
	}

	$effect(() => {
		loadData();
		const interval = setInterval(loadData, 15000);
		return () => clearInterval(interval);
	});
</script>

<div class="flex h-screen w-screen overflow-hidden bg-surface-950">
	<Sidebar />
	<div class="flex flex-col flex-1 min-w-0">
		<TopBar />
		<main class="flex-1 overflow-y-auto">
			<div class="p-6 max-w-7xl mx-auto">
				{@render children()}
			</div>
		</main>
	</div>
</div>
