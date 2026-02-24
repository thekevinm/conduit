<script lang="ts">
	import { page } from '$app/state';
	import { base } from '$app/paths';
	import { app } from '$lib/stores/app.svelte';

	const links = [
		{
			href: `${base}/`,
			label: 'Dashboard',
			icon: 'dashboard'
		},
		{
			href: `${base}/setup`,
			label: 'Setup',
			icon: 'setup'
		},
		{
			href: `${base}/sources`,
			label: 'Sources',
			icon: 'sources'
		},
		{
			href: `${base}/security`,
			label: 'Security',
			icon: 'security'
		},
		{
			href: `${base}/endpoints`,
			label: 'Endpoints',
			icon: 'endpoints'
		}
	];

	function isActive(href: string): boolean {
		const path = page.url?.pathname ?? '';
		if (href === `${base}/`) {
			return path === `${base}` || path === `${base}/`;
		}
		return path.startsWith(href);
	}
</script>

<aside
	class="flex flex-col h-full bg-surface-900/60 border-r border-surface-700/50 transition-all duration-200
		{app.sidebarCollapsed ? 'w-16' : 'w-56'}"
>
	<!-- Logo -->
	<div class="flex items-center gap-3 px-4 h-14 border-b border-surface-700/50">
		<div class="flex-shrink-0 w-7 h-7 rounded-md bg-accent-500 flex items-center justify-center">
			<svg class="w-4 h-4 text-surface-950" fill="currentColor" viewBox="0 0 24 24">
				<path d="M6 8a2 2 0 012-2h8a2 2 0 012 2v2l-4 4 4 4v2a2 2 0 01-2 2H8a2 2 0 01-2-2v-2l4-4-4-4V8z"/>
			</svg>
		</div>
		{#if !app.sidebarCollapsed}
			<span class="font-semibold text-slate-100 tracking-tight text-lg">Conduit</span>
		{/if}
	</div>

	<!-- Navigation -->
	<nav class="flex-1 px-2 py-3 space-y-0.5 overflow-y-auto">
		{#each links as link}
			{@const active = isActive(link.href)}
			<a
				href={link.href}
				class="flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-all duration-150
					{active
						? 'bg-accent-500/10 text-accent-400 border border-accent-500/20'
						: 'text-slate-400 hover:text-slate-200 hover:bg-surface-800/50 border border-transparent'}"
				title={app.sidebarCollapsed ? link.label : undefined}
			>
				<span class="flex-shrink-0 w-5 h-5">
					{#if link.icon === 'dashboard'}
						<svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
							<path stroke-linecap="round" stroke-linejoin="round" d="M3.75 6A2.25 2.25 0 016 3.75h2.25A2.25 2.25 0 0110.5 6v2.25a2.25 2.25 0 01-2.25 2.25H6a2.25 2.25 0 01-2.25-2.25V6zM3.75 15.75A2.25 2.25 0 016 13.5h2.25a2.25 2.25 0 012.25 2.25V18a2.25 2.25 0 01-2.25 2.25H6A2.25 2.25 0 013.75 18v-2.25zM13.5 6a2.25 2.25 0 012.25-2.25H18A2.25 2.25 0 0120.25 6v2.25A2.25 2.25 0 0118 10.5h-2.25a2.25 2.25 0 01-2.25-2.25V6zM13.5 15.75a2.25 2.25 0 012.25-2.25H18a2.25 2.25 0 012.25 2.25V18A2.25 2.25 0 0118 20.25h-2.25A2.25 2.25 0 0113.5 18v-2.25z" />
						</svg>
					{:else if link.icon === 'setup'}
						<svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
							<path stroke-linecap="round" stroke-linejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09zM18.259 8.715L18 9.75l-.259-1.035a3.375 3.375 0 00-2.455-2.456L14.25 6l1.036-.259a3.375 3.375 0 002.455-2.456L18 2.25l.259 1.035a3.375 3.375 0 002.455 2.456L21.75 6l-1.036.259a3.375 3.375 0 00-2.455 2.456z" />
						</svg>
					{:else if link.icon === 'sources'}
						<svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
							<path stroke-linecap="round" stroke-linejoin="round" d="M20.25 6.375c0 2.278-3.694 4.125-8.25 4.125S3.75 8.653 3.75 6.375m16.5 0c0-2.278-3.694-4.125-8.25-4.125S3.75 4.097 3.75 6.375m16.5 0v11.25c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125V6.375m16.5 0v3.75m-16.5-3.75v3.75m16.5 0v3.75C20.25 16.153 16.556 18 12 18s-8.25-1.847-8.25-4.125v-3.75m16.5 0c0 2.278-3.694 4.125-8.25 4.125s-8.25-1.847-8.25-4.125" />
						</svg>
					{:else if link.icon === 'security'}
						<svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
							<path stroke-linecap="round" stroke-linejoin="round" d="M9 12.75L11.25 15 15 9.75m-3-7.036A11.959 11.959 0 013.598 6 11.99 11.99 0 003 9.749c0 5.592 3.824 10.29 9 11.623 5.176-1.332 9-6.03 9-11.622 0-1.31-.21-2.571-.598-3.751h-.152c-3.196 0-6.1-1.248-8.25-3.285z" />
						</svg>
					{:else if link.icon === 'endpoints'}
						<svg fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
							<path stroke-linecap="round" stroke-linejoin="round" d="M13.19 8.688a4.5 4.5 0 011.242 7.244l-4.5 4.5a4.5 4.5 0 01-6.364-6.364l1.757-1.757m13.35-.622l1.757-1.757a4.5 4.5 0 00-6.364-6.364l-4.5 4.5a4.5 4.5 0 001.242 7.244" />
						</svg>
					{/if}
				</span>
				{#if !app.sidebarCollapsed}
					<span>{link.label}</span>
				{/if}
			</a>
		{/each}
	</nav>

	<!-- Collapse toggle -->
	<div class="px-2 py-3 border-t border-surface-700/50">
		<button
			onclick={() => app.toggleSidebar()}
			aria-label={app.sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
			class="flex items-center justify-center w-full px-3 py-2 rounded-lg text-slate-500 hover:text-slate-300 hover:bg-surface-800/50 transition-colors"
		>
			<svg class="w-4 h-4 transition-transform {app.sidebarCollapsed ? 'rotate-180' : ''}" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
				<path stroke-linecap="round" stroke-linejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
			</svg>
		</button>
	</div>
</aside>
