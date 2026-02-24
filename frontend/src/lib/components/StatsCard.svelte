<script lang="ts">
	import type { Snippet } from 'svelte';

	let {
		label,
		value,
		suffix = '',
		trend = '',
		icon
	}: {
		label: string;
		value: string | number;
		suffix?: string;
		trend?: string;
		icon?: Snippet;
	} = $props();
</script>

<div class="group relative bg-surface-900 border border-surface-700/50 rounded-xl p-5 hover:border-surface-600/70 transition-all duration-200">
	<div class="flex items-start justify-between mb-3">
		<span class="text-sm text-slate-400 font-medium">{label}</span>
		{#if icon}
			<div class="text-surface-600 group-hover:text-accent-500 transition-colors">
				{@render icon()}
			</div>
		{/if}
	</div>
	<div class="flex items-baseline gap-1.5">
		<span class="text-3xl font-semibold text-slate-100 tracking-tight font-mono">{value}</span>
		{#if suffix}
			<span class="text-sm text-slate-500">{suffix}</span>
		{/if}
	</div>
	{#if trend}
		<div class="mt-2 text-xs text-slate-500">{trend}</div>
	{/if}
	<div class="absolute inset-x-0 bottom-0 h-px bg-gradient-to-r from-transparent via-accent-500/20 to-transparent opacity-0 group-hover:opacity-100 transition-opacity"></div>
</div>
