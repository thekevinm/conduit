<script lang="ts">
	import { CodeBlock, CopyButton, StatsCard } from '$lib/components';
	import { fetchEndpoints, fetchClientSnippet } from '$lib/api';
	import type { EndpointInfo } from '$lib/api';

	let endpoints = $state<EndpointInfo[]>([]);
	let selectedClient = $state<'claude-code' | 'cursor' | 'vscode' | 'claude-desktop'>('claude-code');
	let snippets = $state<Record<string, string>>({});
	let loadingSnippet = $state(false);

	// Demo data
	const demoEndpoints: EndpointInfo[] = [
		{ url: 'http://localhost:8080/mcp', transport: 'http', status: 'active', requests_today: 47 },
		{ url: 'stdio', transport: 'stdio', status: 'active', requests_today: 0 }
	];

	const demoSnippets: Record<string, string> = {
		'claude-code': `# Add Conduit as an MCP server
claude mcp add conduit -- npx conduit-mcp@latest \\
  --dsn "postgresql://user:pass@localhost:5432/mydb"

# Or connect to an HTTP endpoint
claude mcp add conduit \\
  --transport http \\
  --url http://localhost:8080/mcp`,
		'cursor': `// .cursor/mcp.json
{
  "mcpServers": {
    "conduit": {
      "url": "http://localhost:8080/mcp",
      "headers": {
        "Authorization": "Bearer <your-api-key>"
      }
    }
  }
}`,
		'vscode': `// .vscode/settings.json
{
  "mcp": {
    "servers": {
      "conduit": {
        "type": "http",
        "url": "http://localhost:8080/mcp",
        "headers": {
          "Authorization": "Bearer <your-api-key>"
        }
      }
    }
  }
}`,
		'claude-desktop': `// claude_desktop_config.json
{
  "mcpServers": {
    "conduit": {
      "command": "npx",
      "args": [
        "conduit-mcp@latest",
        "--dsn",
        "postgresql://user:pass@localhost:5432/mydb"
      ]
    }
  }
}`
	};

	$effect(() => {
		loadEndpoints();
	});

	async function loadEndpoints() {
		try {
			endpoints = await fetchEndpoints();
		} catch {
			endpoints = demoEndpoints;
		}
	}

	$effect(() => {
		loadSnippet(selectedClient);
	});

	async function loadSnippet(client: string) {
		if (snippets[client]) return;
		loadingSnippet = true;
		try {
			const result = await fetchClientSnippet(client);
			snippets = { ...snippets, [client]: result.snippet };
		} catch {
			snippets = { ...snippets, [client]: demoSnippets[client] ?? '' };
		}
		loadingSnippet = false;
	}

	const activeEndpoint = $derived(endpoints.find((e) => e.status === 'active'));
	const currentSnippet = $derived(snippets[selectedClient] ?? demoSnippets[selectedClient] ?? '');
</script>

<svelte:head>
	<title>Endpoints — Conduit</title>
</svelte:head>

<div class="space-y-8">
	<div>
		<h1 class="text-2xl font-semibold text-slate-100 tracking-tight">Endpoints</h1>
		<p class="mt-1 text-sm text-slate-400">MCP endpoint configuration and AI client setup</p>
	</div>

	<!-- Active Endpoints -->
	<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
		{#each endpoints.length > 0 ? endpoints : demoEndpoints as ep}
			<div class="bg-surface-900 border border-surface-700/50 rounded-xl p-5">
				<div class="flex items-center justify-between mb-4">
					<div class="flex items-center gap-2">
						<span class="relative flex h-2 w-2">
							{#if ep.status === 'active'}
								<span class="absolute inset-0 rounded-full bg-emerald-400 opacity-40 animate-ping"></span>
							{/if}
							<span class="relative inline-flex h-2 w-2 rounded-full {ep.status === 'active' ? 'bg-emerald-400' : 'bg-slate-600'}"></span>
						</span>
						<span class="text-sm font-medium {ep.status === 'active' ? 'text-emerald-400' : 'text-slate-500'}">
							{ep.status === 'active' ? 'Active' : 'Inactive'}
						</span>
					</div>
					<span class="px-2 py-0.5 rounded text-xs font-mono font-medium bg-surface-800 text-slate-400 border border-surface-700/50 uppercase">
						{ep.transport}
					</span>
				</div>

				{#if ep.transport === 'http'}
					<div class="flex items-center gap-2 p-3 bg-surface-800/80 rounded-lg border border-surface-700/30 mb-4">
						<code class="flex-1 text-sm font-mono text-amber-300/90 truncate">{ep.url}</code>
						<CopyButton text={ep.url} label="" />
					</div>
				{:else}
					<div class="flex items-center gap-2 p-3 bg-surface-800/80 rounded-lg border border-surface-700/30 mb-4">
						<code class="flex-1 text-sm font-mono text-slate-400">Standard I/O (subprocess)</code>
					</div>
				{/if}

				<div class="flex items-center justify-between text-xs text-slate-500">
					<span>Requests today</span>
					<span class="font-mono text-slate-300">{ep.requests_today}</span>
				</div>
			</div>
		{/each}
	</div>

	<!-- Usage Stats -->
	<div class="grid grid-cols-1 sm:grid-cols-3 gap-4">
		<StatsCard label="Total Requests" value={endpoints.reduce((a, e) => a + e.requests_today, 0) || 47}>
			{#snippet icon()}
				<svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
					<path stroke-linecap="round" stroke-linejoin="round" d="M7.5 21L3 16.5m0 0L7.5 12M3 16.5h13.5m0-13.5L21 7.5m0 0L16.5 12M21 7.5H7.5" />
				</svg>
			{/snippet}
		</StatsCard>
		<StatsCard label="Active Transports" value={endpoints.filter((e) => e.status === 'active').length || 2}>
			{#snippet icon()}
				<svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
					<path stroke-linecap="round" stroke-linejoin="round" d="M8.288 15.038a5.25 5.25 0 017.424 0M5.106 11.856c3.807-3.808 9.98-3.808 13.788 0M1.924 8.674c5.565-5.565 14.587-5.565 20.152 0M12.53 18.22l-.53.53-.53-.53a.75.75 0 011.06 0z" />
				</svg>
			{/snippet}
		</StatsCard>
		<StatsCard label="Avg Response" value="23" suffix="ms">
			{#snippet icon()}
				<svg class="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
					<path stroke-linecap="round" stroke-linejoin="round" d="M3.75 13.5l10.5-11.25L12 10.5h8.25L9.75 21.75 12 13.5H3.75z" />
				</svg>
			{/snippet}
		</StatsCard>
	</div>

	<!-- Client Config Snippets -->
	<div class="bg-surface-900 border border-surface-700/50 rounded-xl overflow-hidden">
		<div class="px-5 py-4 border-b border-surface-700/50">
			<h2 class="text-sm font-semibold text-slate-200 uppercase tracking-wider">Connect Your AI</h2>
			<p class="mt-1 text-xs text-slate-500">Copy the configuration snippet for your preferred AI client</p>
		</div>

		<div class="p-5 space-y-4">
			<!-- Client tabs -->
			<div class="flex gap-1 p-1 bg-surface-800/50 rounded-lg border border-surface-700/30">
				{#each [
					{ id: 'claude-code', label: 'Claude Code', icon: '>' },
					{ id: 'cursor', label: 'Cursor', icon: '{}' },
					{ id: 'vscode', label: 'VS Code', icon: '{}' },
					{ id: 'claude-desktop', label: 'Claude Desktop', icon: '{}' }
				] as tab}
					<button
						onclick={() => (selectedClient = tab.id as typeof selectedClient)}
						class="flex items-center gap-2 flex-1 px-3 py-2.5 rounded-md text-sm font-medium transition-all
							{selectedClient === tab.id
								? 'bg-surface-700 text-accent-400 shadow-sm'
								: 'text-slate-500 hover:text-slate-300'}"
					>
						<span class="font-mono text-xs text-surface-600">{tab.icon}</span>
						{tab.label}
					</button>
				{/each}
			</div>

			<!-- Snippet -->
			{#if loadingSnippet}
				<div class="flex items-center justify-center py-8 text-slate-500 text-sm">
					Loading snippet...
				</div>
			{:else}
				<CodeBlock
					code={currentSnippet}
					title={selectedClient === 'claude-code' ? 'Terminal' : 'Configuration File'}
					language={selectedClient === 'claude-code' ? 'bash' : 'json'}
				/>
			{/if}

			<!-- Instructions -->
			<div class="p-4 rounded-lg bg-surface-800/30 border border-surface-700/20">
				{#if selectedClient === 'claude-code'}
					<h3 class="text-sm font-medium text-slate-300 mb-2">Setup Instructions</h3>
					<ol class="text-xs text-slate-500 space-y-1.5 list-decimal list-inside">
						<li>Run the command above in your terminal</li>
						<li>Claude Code will automatically discover the MCP tools</li>
						<li>Try asking: "Show me all tables in my database"</li>
					</ol>
				{:else if selectedClient === 'cursor'}
					<h3 class="text-sm font-medium text-slate-300 mb-2">Setup Instructions</h3>
					<ol class="text-xs text-slate-500 space-y-1.5 list-decimal list-inside">
						<li>Create or edit <code class="font-mono text-amber-300/70">.cursor/mcp.json</code> in your project root</li>
						<li>Paste the configuration above</li>
						<li>Replace <code class="font-mono text-amber-300/70">&lt;your-api-key&gt;</code> with your API key from the Security page</li>
						<li>Restart Cursor to load the MCP server</li>
					</ol>
				{:else if selectedClient === 'vscode'}
					<h3 class="text-sm font-medium text-slate-300 mb-2">Setup Instructions</h3>
					<ol class="text-xs text-slate-500 space-y-1.5 list-decimal list-inside">
						<li>Open VS Code Settings (JSON)</li>
						<li>Add the MCP configuration from above</li>
						<li>Replace <code class="font-mono text-amber-300/70">&lt;your-api-key&gt;</code> with your API key</li>
						<li>The MCP server will connect on next activation</li>
					</ol>
				{:else}
					<h3 class="text-sm font-medium text-slate-300 mb-2">Setup Instructions</h3>
					<ol class="text-xs text-slate-500 space-y-1.5 list-decimal list-inside">
						<li>Open Claude Desktop and go to Settings &rarr; Developer</li>
						<li>Edit your <code class="font-mono text-amber-300/70">claude_desktop_config.json</code></li>
						<li>Add the server configuration above</li>
						<li>Restart Claude Desktop</li>
					</ol>
				{/if}
			</div>
		</div>
	</div>
</div>
