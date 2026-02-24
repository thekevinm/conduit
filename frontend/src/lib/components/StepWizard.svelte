<script lang="ts">
	import type { Snippet } from 'svelte';

	interface Step {
		label: string;
		description?: string;
	}

	let {
		steps,
		currentStep = 0,
		children
	}: {
		steps: Step[];
		currentStep?: number;
		children: Snippet;
	} = $props();
</script>

<div class="space-y-8">
	<!-- Progress bar -->
	<div class="relative">
		<div class="flex items-center justify-between">
			{#each steps as step, i}
				<div class="flex flex-col items-center z-10 relative">
					<div
						class="w-9 h-9 rounded-full flex items-center justify-center text-sm font-mono font-bold border-2 transition-all duration-300
						{i < currentStep
							? 'bg-accent-500 border-accent-500 text-surface-950'
							: i === currentStep
								? 'bg-surface-900 border-accent-500 text-accent-400 ring-4 ring-accent-500/20'
								: 'bg-surface-900 border-surface-700 text-surface-600'}"
					>
						{#if i < currentStep}
							<svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="3">
								<path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" />
							</svg>
						{:else}
							{i + 1}
						{/if}
					</div>
					<span class="mt-2 text-xs font-medium {i <= currentStep ? 'text-slate-300' : 'text-surface-600'}">
						{step.label}
					</span>
				</div>
				{#if i < steps.length - 1}
					<div class="flex-1 h-0.5 mx-2 -mt-6 {i < currentStep ? 'bg-accent-500' : 'bg-surface-700'} transition-colors duration-300"></div>
				{/if}
			{/each}
		</div>
	</div>

	<!-- Step content -->
	<div class="min-h-[300px]">
		{@render children()}
	</div>
</div>
