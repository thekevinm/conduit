<script lang="ts">
	let {
		status = 'connected',
		label = '',
		class: className = ''
	}: {
		status?: 'connected' | 'disconnected' | 'error' | 'loading';
		label?: string;
		class?: string;
	} = $props();

	const statusConfig = $derived({
		connected: { dot: 'bg-emerald-400', ring: 'ring-emerald-400/30', text: 'text-emerald-400', defaultLabel: 'Connected' },
		disconnected: { dot: 'bg-slate-500', ring: 'ring-slate-500/30', text: 'text-slate-400', defaultLabel: 'Disconnected' },
		error: { dot: 'bg-rose-400', ring: 'ring-rose-400/30', text: 'text-rose-400', defaultLabel: 'Error' },
		loading: { dot: 'bg-amber-400 animate-pulse', ring: 'ring-amber-400/30', text: 'text-amber-400', defaultLabel: 'Connecting...' }
	}[status]);

	const displayLabel = $derived(label || statusConfig.defaultLabel);
</script>

<span
	class="inline-flex items-center gap-2 px-3 py-1.5 rounded-full bg-surface-800/80 border border-surface-700/50 text-sm {className}"
>
	<span class="relative flex h-2 w-2">
		{#if status === 'connected'}
			<span class="absolute inset-0 rounded-full {statusConfig.dot} opacity-40 animate-ping"></span>
		{/if}
		<span class="relative inline-flex h-2 w-2 rounded-full {statusConfig.dot} ring-2 {statusConfig.ring}"></span>
	</span>
	<span class="{statusConfig.text} font-medium">{displayLabel}</span>
</span>
