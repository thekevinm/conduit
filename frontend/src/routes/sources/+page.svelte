<script lang="ts">
	import { base } from '$app/paths';
	import { DatabaseIcon, StatusPill } from '$lib/components';
	import { app } from '$lib/stores/app.svelte';
	import type { Source, TableInfo, ColumnInfo } from '$lib/api';
	import { fetchSourceTables, removeSource } from '$lib/api';

	let selectedSource = $state<Source | null>(null);
	let tables = $state<TableInfo[]>([]);
	let expandedTables = $state<Set<string>>(new Set());
	let loadingTables = $state(false);
	let searchQuery = $state('');

	// Demo data for development
	const demoSources: Source[] = [
		{
			id: 'demo-pg',
			name: 'Production DB',
			type: 'PostgreSQL',
			host: 'db.example.com',
			port: 5432,
			database: 'myapp',
			tables: 24,
			status: 'connected',
			access_mode: 'read-only',
			created_at: '2026-02-24T10:00:00Z'
		},
		{
			id: 'demo-mysql',
			name: 'Analytics',
			type: 'MySQL',
			host: 'analytics.internal',
			port: 3306,
			database: 'analytics',
			tables: 8,
			status: 'connected',
			access_mode: 'read-only',
			created_at: '2026-02-24T11:00:00Z'
		}
	];

	const demoTables: TableInfo[] = [
		{
			name: 'users',
			schema: 'public',
			row_count: 15420,
			columns: [
				{ name: 'id', type: 'uuid', nullable: false, default_value: 'gen_random_uuid()', is_primary_key: true, is_foreign_key: false },
				{ name: 'email', type: 'varchar(255)', nullable: false, default_value: null, is_primary_key: false, is_foreign_key: false },
				{ name: 'name', type: 'varchar(128)', nullable: true, default_value: null, is_primary_key: false, is_foreign_key: false },
				{ name: 'created_at', type: 'timestamptz', nullable: false, default_value: 'now()', is_primary_key: false, is_foreign_key: false },
				{ name: 'org_id', type: 'uuid', nullable: true, default_value: null, is_primary_key: false, is_foreign_key: true }
			],
			primary_key: ['id'],
			indexes: [
				{ name: 'users_pkey', columns: ['id'], unique: true },
				{ name: 'users_email_idx', columns: ['email'], unique: true }
			],
			foreign_keys: [
				{ column: 'org_id', references_table: 'organizations', references_column: 'id' }
			]
		},
		{
			name: 'orders',
			schema: 'public',
			row_count: 89234,
			columns: [
				{ name: 'id', type: 'bigserial', nullable: false, default_value: null, is_primary_key: true, is_foreign_key: false },
				{ name: 'user_id', type: 'uuid', nullable: false, default_value: null, is_primary_key: false, is_foreign_key: true },
				{ name: 'total', type: 'numeric(10,2)', nullable: false, default_value: '0.00', is_primary_key: false, is_foreign_key: false },
				{ name: 'status', type: 'varchar(32)', nullable: false, default_value: "'pending'", is_primary_key: false, is_foreign_key: false },
				{ name: 'created_at', type: 'timestamptz', nullable: false, default_value: 'now()', is_primary_key: false, is_foreign_key: false }
			],
			primary_key: ['id'],
			indexes: [
				{ name: 'orders_pkey', columns: ['id'], unique: true },
				{ name: 'orders_user_id_idx', columns: ['user_id'], unique: false },
				{ name: 'orders_status_idx', columns: ['status'], unique: false }
			],
			foreign_keys: [
				{ column: 'user_id', references_table: 'users', references_column: 'id' }
			]
		},
		{
			name: 'products',
			schema: 'public',
			row_count: 1247,
			columns: [
				{ name: 'id', type: 'uuid', nullable: false, default_value: 'gen_random_uuid()', is_primary_key: true, is_foreign_key: false },
				{ name: 'name', type: 'varchar(256)', nullable: false, default_value: null, is_primary_key: false, is_foreign_key: false },
				{ name: 'price', type: 'numeric(10,2)', nullable: false, default_value: null, is_primary_key: false, is_foreign_key: false },
				{ name: 'category', type: 'varchar(64)', nullable: true, default_value: null, is_primary_key: false, is_foreign_key: false }
			],
			primary_key: ['id'],
			indexes: [
				{ name: 'products_pkey', columns: ['id'], unique: true }
			],
			foreign_keys: []
		},
		{
			name: 'organizations',
			schema: 'public',
			row_count: 342,
			columns: [
				{ name: 'id', type: 'uuid', nullable: false, default_value: 'gen_random_uuid()', is_primary_key: true, is_foreign_key: false },
				{ name: 'name', type: 'varchar(256)', nullable: false, default_value: null, is_primary_key: false, is_foreign_key: false },
				{ name: 'plan', type: 'varchar(32)', nullable: false, default_value: "'free'", is_primary_key: false, is_foreign_key: false }
			],
			primary_key: ['id'],
			indexes: [
				{ name: 'organizations_pkey', columns: ['id'], unique: true }
			],
			foreign_keys: []
		}
	];

	const displaySources = $derived(app.sources.length > 0 ? app.sources : demoSources);

	async function selectSource(source: Source) {
		selectedSource = source;
		loadingTables = true;
		expandedTables = new Set();
		try {
			tables = await fetchSourceTables(source.id);
		} catch {
			tables = demoTables;
		}
		loadingTables = false;
	}

	function toggleTable(name: string) {
		const next = new Set(expandedTables);
		if (next.has(name)) {
			next.delete(name);
		} else {
			next.add(name);
		}
		expandedTables = next;
	}

	async function handleRemoveSource(id: string) {
		try {
			await removeSource(id);
			app.sources = app.sources.filter((s) => s.id !== id);
			if (selectedSource?.id === id) {
				selectedSource = null;
				tables = [];
			}
		} catch {
			// Ignore in dev
		}
	}

	const filteredTables = $derived(
		searchQuery
			? tables.filter((t) => t.name.toLowerCase().includes(searchQuery.toLowerCase()))
			: tables
	);

	function formatRowCount(n: number): string {
		if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
		if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
		return n.toString();
	}
</script>

<svelte:head>
	<title>Sources — Conduit</title>
</svelte:head>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div>
			<h1 class="text-2xl font-semibold text-slate-100 tracking-tight">Sources</h1>
			<p class="mt-1 text-sm text-slate-400">Manage your database connections</p>
		</div>
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

	<!-- Source Cards -->
	<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
		{#each displaySources as source}
			<button
				onclick={() => selectSource(source)}
				class="flex flex-col p-4 rounded-xl border-2 text-left transition-all duration-200 group
					{selectedSource?.id === source.id
						? 'border-accent-500/50 bg-accent-500/5'
						: 'border-surface-700/50 bg-surface-900 hover:border-surface-600'}"
			>
				<div class="flex items-center gap-3 mb-3">
					<DatabaseIcon type={source.type} size="md" />
					<div class="flex-1 min-w-0">
						<h3 class="text-sm font-semibold text-slate-200 truncate">{source.name}</h3>
						<p class="text-xs text-slate-500 font-mono truncate">{source.host}:{source.port}/{source.database}</p>
					</div>
					<StatusPill status={source.status} label="" class="scale-75 origin-right" />
				</div>
				<div class="flex items-center justify-between text-xs">
					<span class="text-slate-500">{source.tables} tables</span>
					<span class="px-2 py-0.5 rounded bg-surface-800 text-slate-400 font-mono">{source.access_mode}</span>
				</div>
			</button>
		{/each}
	</div>

	<!-- Table Explorer -->
	{#if selectedSource}
		<div class="bg-surface-900 border border-surface-700/50 rounded-xl overflow-hidden">
			<div class="flex items-center justify-between px-5 py-4 border-b border-surface-700/50">
				<div class="flex items-center gap-3">
					<DatabaseIcon type={selectedSource.type} size="sm" />
					<h2 class="text-sm font-semibold text-slate-200">{selectedSource.name}</h2>
					<span class="text-xs text-slate-500">Schema Explorer</span>
				</div>
				<div class="flex items-center gap-3">
					<div class="relative">
						<svg class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-surface-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
							<path stroke-linecap="round" stroke-linejoin="round" d="M21 21l-5.197-5.197m0 0A7.5 7.5 0 105.196 5.196a7.5 7.5 0 0010.607 10.607z" />
						</svg>
						<input
							type="text"
							bind:value={searchQuery}
							class="pl-9 pr-3 py-1.5 bg-surface-800 border border-surface-700 rounded-lg text-sm text-slate-300 placeholder:text-surface-600 focus:outline-none focus:border-accent-500/50 w-48"
							placeholder="Filter tables..."
						/>
					</div>
					<button
						onclick={() => handleRemoveSource(selectedSource!.id)}
						class="text-xs text-rose-400/60 hover:text-rose-400 transition-colors"
					>
						Remove
					</button>
				</div>
			</div>

			{#if loadingTables}
				<div class="flex items-center justify-center py-12 text-slate-500">
					<svg class="w-5 h-5 animate-spin mr-2" fill="none" viewBox="0 0 24 24">
						<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
						<path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
					</svg>
					Loading schema...
				</div>
			{:else}
				<div class="divide-y divide-surface-800/50">
					{#each filteredTables as table}
						<div>
							<button
								onclick={() => toggleTable(table.name)}
								class="flex items-center gap-3 w-full px-5 py-3 text-left hover:bg-surface-800/30 transition-colors group"
							>
								<svg
									class="w-4 h-4 text-surface-600 transition-transform {expandedTables.has(table.name) ? 'rotate-90' : ''}"
									fill="none"
									viewBox="0 0 24 24"
									stroke="currentColor"
									stroke-width="2"
								>
									<path stroke-linecap="round" stroke-linejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
								</svg>
								<svg class="w-4 h-4 text-surface-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
									<path stroke-linecap="round" stroke-linejoin="round" d="M3.375 19.5h17.25m-17.25 0a1.125 1.125 0 01-1.125-1.125M3.375 19.5h7.5c.621 0 1.125-.504 1.125-1.125m-9.75 0V5.625m0 12.75v-1.5c0-.621.504-1.125 1.125-1.125m18.375 2.625V5.625m0 12.75c0 .621-.504 1.125-1.125 1.125m1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125" />
								</svg>
								<span class="font-mono text-sm text-amber-300/90">{table.schema}.{table.name}</span>
								<span class="text-xs text-slate-500 ml-auto">{table.columns.length} cols</span>
								<span class="text-xs text-slate-600 font-mono">{formatRowCount(table.row_count)} rows</span>
							</button>

							{#if expandedTables.has(table.name)}
								<div class="px-5 pb-4 pl-16">
									<!-- Columns -->
									<div class="rounded-lg border border-surface-700/30 overflow-hidden">
										<table class="w-full text-xs">
											<thead>
												<tr class="bg-surface-800/50">
													<th class="px-3 py-2 text-left text-slate-500 font-medium uppercase tracking-wider">Column</th>
													<th class="px-3 py-2 text-left text-slate-500 font-medium uppercase tracking-wider">Type</th>
													<th class="px-3 py-2 text-center text-slate-500 font-medium uppercase tracking-wider">Null</th>
													<th class="px-3 py-2 text-left text-slate-500 font-medium uppercase tracking-wider">Default</th>
													<th class="px-3 py-2 text-center text-slate-500 font-medium uppercase tracking-wider">Keys</th>
												</tr>
											</thead>
											<tbody class="divide-y divide-surface-800/30">
												{#each table.columns as col}
													<tr class="hover:bg-surface-800/20">
														<td class="px-3 py-2 font-mono text-slate-200">
															{col.name}
														</td>
														<td class="px-3 py-2 font-mono text-amber-300/70">{col.type}</td>
														<td class="px-3 py-2 text-center">
															{#if col.nullable}
																<span class="text-slate-600">yes</span>
															{:else}
																<span class="text-slate-400">no</span>
															{/if}
														</td>
														<td class="px-3 py-2 font-mono text-slate-500">{col.default_value ?? '—'}</td>
														<td class="px-3 py-2 text-center">
															{#if col.is_primary_key}
																<span class="px-1.5 py-0.5 rounded bg-amber-500/15 text-amber-400 font-bold">PK</span>
															{/if}
															{#if col.is_foreign_key}
																<span class="px-1.5 py-0.5 rounded bg-blue-500/15 text-blue-400 font-bold">FK</span>
															{/if}
														</td>
													</tr>
												{/each}
											</tbody>
										</table>
									</div>

									<!-- Indexes & Foreign Keys -->
									{#if table.indexes.length > 0 || table.foreign_keys.length > 0}
										<div class="mt-3 grid grid-cols-2 gap-3">
											{#if table.indexes.length > 0}
												<div>
													<h4 class="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1.5">Indexes</h4>
													<div class="space-y-1">
														{#each table.indexes as idx}
															<div class="flex items-center gap-2 text-xs">
																{#if idx.unique}
																	<span class="px-1 py-0.5 rounded bg-amber-500/10 text-amber-400 font-mono text-[10px]">UQ</span>
																{:else}
																	<span class="px-1 py-0.5 rounded bg-surface-700/50 text-slate-500 font-mono text-[10px]">IX</span>
																{/if}
																<span class="font-mono text-slate-400 truncate">{idx.name}</span>
															</div>
														{/each}
													</div>
												</div>
											{/if}
											{#if table.foreign_keys.length > 0}
												<div>
													<h4 class="text-xs font-medium text-slate-500 uppercase tracking-wider mb-1.5">Foreign Keys</h4>
													<div class="space-y-1">
														{#each table.foreign_keys as fk}
															<div class="text-xs">
																<span class="font-mono text-slate-400">{fk.column}</span>
																<span class="text-surface-600 mx-1">&rarr;</span>
																<span class="font-mono text-blue-400">{fk.references_table}.{fk.references_column}</span>
															</div>
														{/each}
													</div>
												</div>
											{/if}
										</div>
									{/if}
								</div>
							{/if}
						</div>
					{/each}
				</div>
			{/if}
		</div>
	{/if}
</div>
