<script lang="ts">
	import CopyButton from './CopyButton.svelte';

	let {
		code,
		language = 'text',
		title = '',
		showCopy = true,
		class: className = ''
	}: {
		code: string;
		language?: string;
		title?: string;
		showCopy?: boolean;
		class?: string;
	} = $props();
</script>

<div class="rounded-lg border border-surface-700/50 overflow-hidden {className}">
	{#if title || showCopy}
		<div class="flex items-center justify-between px-4 py-2 bg-surface-800/80 border-b border-surface-700/50">
			{#if title}
				<span class="text-xs font-medium text-slate-400 uppercase tracking-wider">{title}</span>
			{:else}
				<span></span>
			{/if}
			{#if showCopy}
				<CopyButton text={code} />
			{/if}
		</div>
	{/if}
	<div class="relative">
		<pre class="p-4 overflow-x-auto bg-surface-900/80 text-sm leading-relaxed"><code
			class="font-mono text-amber-300/90">{code}</code></pre>
	</div>
</div>
