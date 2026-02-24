<script lang="ts">
	import type { Snippet } from 'svelte';

	interface Column {
		key: string;
		label: string;
		class?: string;
		mono?: boolean;
	}

	let {
		columns,
		rows,
		emptyMessage = 'No data',
		cell,
		class: className = ''
	}: {
		columns: Column[];
		rows: Record<string, unknown>[];
		emptyMessage?: string;
		cell?: Snippet<[{ row: Record<string, unknown>; column: Column; value: unknown }]>;
		class?: string;
	} = $props();
</script>

<div class="overflow-x-auto rounded-lg border border-surface-700/50 {className}">
	<table class="w-full text-sm">
		<thead>
			<tr class="border-b border-surface-700/50 bg-surface-800/50">
				{#each columns as col}
					<th class="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase tracking-wider {col.class ?? ''}">
						{col.label}
					</th>
				{/each}
			</tr>
		</thead>
		<tbody class="divide-y divide-surface-800/50">
			{#if rows.length === 0}
				<tr>
					<td colspan={columns.length} class="px-4 py-8 text-center text-slate-500">
						{emptyMessage}
					</td>
				</tr>
			{:else}
				{#each rows as row}
					<tr class="hover:bg-surface-800/30 transition-colors">
						{#each columns as col}
							<td class="px-4 py-3 {col.mono ? 'font-mono text-amber-300/80' : 'text-slate-300'} {col.class ?? ''}">
								{#if cell}
									{@render cell({ row, column: col, value: row[col.key] })}
								{:else}
									{row[col.key] ?? '—'}
								{/if}
							</td>
						{/each}
					</tr>
				{/each}
			{/if}
		</tbody>
	</table>
</div>
