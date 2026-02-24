<script lang="ts">
	import { goto } from '$app/navigation';
	import { base } from '$app/paths';
	import { StepWizard, DatabaseIcon, CodeBlock, CopyButton } from '$lib/components';
	import { testConnection, addSource } from '$lib/api';

	const steps = [
		{ label: 'Database' },
		{ label: 'Connection' },
		{ label: 'Access' },
		{ label: 'Security' },
		{ label: 'Connect AI' }
	];

	let currentStep = $state(0);

	// Step 1: Database type
	const dbTypes = [
		{ id: 'postgresql', label: 'PostgreSQL', icon: 'postgresql', desc: 'The most advanced open source database' },
		{ id: 'mysql', label: 'MySQL', icon: 'mysql', desc: 'Popular open source relational database' },
		{ id: 'mssql', label: 'SQL Server', icon: 'mssql', desc: 'Microsoft enterprise database' },
		{ id: 'sqlite', label: 'SQLite', icon: 'sqlite', desc: 'Embedded file-based database' },
		{ id: 'oracle', label: 'Oracle', icon: 'oracle', desc: 'Enterprise database platform' },
		{ id: 'snowflake', label: 'Snowflake', icon: 'snowflake', desc: 'Cloud data warehouse' }
	];
	let selectedDb = $state('');

	// Step 2: Connection details
	let connHost = $state('localhost');
	let connPort = $state('5432');
	let connDatabase = $state('');
	let connUser = $state('');
	let connPassword = $state('');
	let connSSL = $state(false);
	let connTesting = $state(false);
	let connTestResult = $state<{ success: boolean; message: string } | null>(null);

	// Step 3: Access
	let accessMode = $state<'read-only' | 'read-write'>('read-only');
	let selectedTables = $state<string[]>([]);
	let allTables = $state(true);

	// Step 4: Security
	let authMode = $state<'none' | 'api-key' | 'bearer'>('none');
	let generatedKey = $state('');

	// Step 5: Connect AI
	let selectedClient = $state<'claude-code' | 'cursor' | 'vscode' | 'claude-desktop'>('claude-code');

	const defaultPorts: Record<string, string> = {
		postgresql: '5432',
		mysql: '3306',
		mssql: '1433',
		sqlite: '',
		oracle: '1521',
		snowflake: '443'
	};

	function selectDb(id: string) {
		selectedDb = id;
		connPort = defaultPorts[id] ?? '5432';
	}

	async function handleTestConnection() {
		connTesting = true;
		connTestResult = null;
		try {
			const result = await testConnection({
				type: selectedDb,
				host: connHost,
				port: parseInt(connPort),
				database: connDatabase,
				user: connUser,
				password: connPassword,
				ssl: connSSL
			});
			connTestResult = result;
		} catch (e) {
			connTestResult = { success: false, message: e instanceof Error ? e.message : 'Connection failed' };
		}
		connTesting = false;
	}

	function generateApiKey() {
		const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
		let key = 'cdt_';
		for (let i = 0; i < 32; i++) {
			key += chars[Math.floor(Math.random() * chars.length)];
		}
		generatedKey = key;
	}

	async function handleFinish() {
		try {
			await addSource({
				type: selectedDb,
				host: connHost,
				port: parseInt(connPort),
				database: connDatabase,
				user: connUser,
				password: connPassword,
				ssl: connSSL,
				access_mode: accessMode,
				tables: allTables ? [] : selectedTables
			});
		} catch {
			// Source may not be addable in dev mode — proceed anyway
		}
		goto(`${base}/`);
	}

	function next() {
		if (currentStep < steps.length - 1) currentStep++;
	}

	function prev() {
		if (currentStep > 0) currentStep--;
	}

	const canProceed = $derived(
		currentStep === 0 ? selectedDb !== '' :
		currentStep === 1 ? connDatabase !== '' && (selectedDb === 'sqlite' || (connHost !== '' && connUser !== '')) :
		true
	);

	const clientSnippets = $derived<Record<string, string>>({
		'claude-code': `claude mcp add conduit -- npx conduit-mcp@latest \\
  --dsn "${selectedDb}://${connUser}:****@${connHost}:${connPort}/${connDatabase}"`,
		'cursor': `{
  "mcpServers": {
    "conduit": {
      "url": "http://localhost:8080/mcp"${authMode === 'api-key' ? `,
      "headers": {
        "Authorization": "Bearer ${generatedKey || '<your-api-key>'}"
      }` : ''}
    }
  }
}`,
		'vscode': `{
  "mcp": {
    "servers": {
      "conduit": {
        "type": "http",
        "url": "http://localhost:8080/mcp"${authMode === 'api-key' ? `,
        "headers": {
          "Authorization": "Bearer ${generatedKey || '<your-api-key>'}"
        }` : ''}
      }
    }
  }
}`,
		'claude-desktop': `{
  "mcpServers": {
    "conduit": {
      "command": "npx",
      "args": ["conduit-mcp@latest", "--dsn", "${selectedDb}://${connUser}:****@${connHost}:${connPort}/${connDatabase}"]
    }
  }
}`
	});
</script>

<svelte:head>
	<title>Setup — Conduit</title>
</svelte:head>

<div class="max-w-3xl mx-auto space-y-8">
	<div>
		<h1 class="text-2xl font-semibold text-slate-100 tracking-tight">Setup Wizard</h1>
		<p class="mt-1 text-sm text-slate-400">Connect your database and start using MCP in minutes</p>
	</div>

	<StepWizard {steps} {currentStep}>
		<!-- Step 1: Database Type -->
		{#if currentStep === 0}
			<div class="space-y-4">
				<p class="text-sm text-slate-400">Choose your database type</p>
				<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
					{#each dbTypes as db}
						<button
							onclick={() => selectDb(db.id)}
							class="flex flex-col items-center gap-3 p-5 rounded-xl border-2 transition-all duration-200
								{selectedDb === db.id
									? 'border-accent-500 bg-accent-500/5 shadow-lg shadow-accent-500/10'
									: 'border-surface-700/50 bg-surface-900 hover:border-surface-600 hover:bg-surface-800/50'}"
						>
							<DatabaseIcon type={db.id} size="lg" />
							<div class="text-center">
								<div class="text-sm font-semibold {selectedDb === db.id ? 'text-accent-400' : 'text-slate-200'}">{db.label}</div>
								<div class="text-xs text-slate-500 mt-0.5">{db.desc}</div>
							</div>
						</button>
					{/each}
				</div>
			</div>

		<!-- Step 2: Connection -->
		{:else if currentStep === 1}
			<div class="space-y-5">
				<p class="text-sm text-slate-400">Enter your database connection details</p>

				{#if selectedDb !== 'sqlite'}
					<div class="grid grid-cols-3 gap-4">
						<div class="col-span-2">
							<label class="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5" for="host">Host</label>
							<input
								id="host"
								type="text"
								bind:value={connHost}
								class="w-full px-3 py-2.5 bg-surface-800 border border-surface-700 rounded-lg text-slate-200 text-sm font-mono focus:outline-none focus:border-accent-500/50 focus:ring-1 focus:ring-accent-500/20 placeholder:text-surface-600"
								placeholder="localhost"
							/>
						</div>
						<div>
							<label class="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5" for="port">Port</label>
							<input
								id="port"
								type="text"
								bind:value={connPort}
								class="w-full px-3 py-2.5 bg-surface-800 border border-surface-700 rounded-lg text-slate-200 text-sm font-mono focus:outline-none focus:border-accent-500/50 focus:ring-1 focus:ring-accent-500/20 placeholder:text-surface-600"
								placeholder="5432"
							/>
						</div>
					</div>
				{/if}

				<div>
					<label class="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5" for="database">Database{selectedDb === 'sqlite' ? ' File Path' : ''}</label>
					<input
						id="database"
						type="text"
						bind:value={connDatabase}
						class="w-full px-3 py-2.5 bg-surface-800 border border-surface-700 rounded-lg text-slate-200 text-sm font-mono focus:outline-none focus:border-accent-500/50 focus:ring-1 focus:ring-accent-500/20 placeholder:text-surface-600"
						placeholder={selectedDb === 'sqlite' ? '/path/to/database.db' : 'myapp'}
					/>
				</div>

				{#if selectedDb !== 'sqlite'}
					<div class="grid grid-cols-2 gap-4">
						<div>
							<label class="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5" for="user">Username</label>
							<input
								id="user"
								type="text"
								bind:value={connUser}
								class="w-full px-3 py-2.5 bg-surface-800 border border-surface-700 rounded-lg text-slate-200 text-sm font-mono focus:outline-none focus:border-accent-500/50 focus:ring-1 focus:ring-accent-500/20 placeholder:text-surface-600"
								placeholder="postgres"
							/>
						</div>
						<div>
							<label class="block text-xs font-medium text-slate-400 uppercase tracking-wider mb-1.5" for="password">Password</label>
							<input
								id="password"
								type="password"
								bind:value={connPassword}
								class="w-full px-3 py-2.5 bg-surface-800 border border-surface-700 rounded-lg text-slate-200 text-sm font-mono focus:outline-none focus:border-accent-500/50 focus:ring-1 focus:ring-accent-500/20 placeholder:text-surface-600"
								placeholder="••••••••"
							/>
						</div>
					</div>

					<label class="flex items-center gap-3 cursor-pointer group">
						<div class="relative">
							<input type="checkbox" bind:checked={connSSL} class="peer sr-only" />
							<div class="w-9 h-5 rounded-full bg-surface-700 peer-checked:bg-accent-500 transition-colors"></div>
							<div class="absolute top-0.5 left-0.5 w-4 h-4 rounded-full bg-white shadow-sm transition-transform peer-checked:translate-x-4"></div>
						</div>
						<span class="text-sm text-slate-400 group-hover:text-slate-300">Require SSL</span>
					</label>
				{/if}

				<div class="flex items-center gap-3 pt-2">
					<button
						onclick={handleTestConnection}
						disabled={connTesting || !connDatabase}
						class="inline-flex items-center gap-2 px-4 py-2 rounded-lg border border-surface-700 text-sm font-medium text-slate-300 hover:bg-surface-800 hover:border-surface-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
					>
						{#if connTesting}
							<svg class="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24">
								<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
								<path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path>
							</svg>
							Testing...
						{:else}
							Test Connection
						{/if}
					</button>
					{#if connTestResult}
						<span class="text-sm {connTestResult.success ? 'text-emerald-400' : 'text-rose-400'}">
							{connTestResult.message}
						</span>
					{/if}
				</div>
			</div>

		<!-- Step 3: Access -->
		{:else if currentStep === 2}
			<div class="space-y-6">
				<p class="text-sm text-slate-400">Configure access permissions for your MCP tools</p>

				<div class="space-y-3">
					<h3 class="text-sm font-medium text-slate-300">Access Mode</h3>
					<div class="grid grid-cols-2 gap-3">
						<button
							onclick={() => (accessMode = 'read-only')}
							class="flex flex-col items-start gap-2 p-4 rounded-xl border-2 transition-all
								{accessMode === 'read-only'
									? 'border-accent-500 bg-accent-500/5'
									: 'border-surface-700/50 bg-surface-900 hover:border-surface-600'}"
						>
							<div class="flex items-center gap-2">
								<svg class="w-5 h-5 {accessMode === 'read-only' ? 'text-accent-400' : 'text-slate-500'}" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
									<path stroke-linecap="round" stroke-linejoin="round" d="M2.036 12.322a1.012 1.012 0 010-.639C3.423 7.51 7.36 4.5 12 4.5c4.638 0 8.573 3.007 9.963 7.178.07.207.07.431 0 .639C20.577 16.49 16.64 19.5 12 19.5c-4.638 0-8.573-3.007-9.963-7.178z" />
									<path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
								</svg>
								<span class="text-sm font-semibold {accessMode === 'read-only' ? 'text-accent-400' : 'text-slate-300'}">Read Only</span>
							</div>
							<span class="text-xs text-slate-500">SELECT queries only. Safest option for production databases.</span>
						</button>
						<button
							onclick={() => (accessMode = 'read-write')}
							class="flex flex-col items-start gap-2 p-4 rounded-xl border-2 transition-all
								{accessMode === 'read-write'
									? 'border-accent-500 bg-accent-500/5'
									: 'border-surface-700/50 bg-surface-900 hover:border-surface-600'}"
						>
							<div class="flex items-center gap-2">
								<svg class="w-5 h-5 {accessMode === 'read-write' ? 'text-accent-400' : 'text-slate-500'}" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="1.5">
									<path stroke-linecap="round" stroke-linejoin="round" d="M16.862 4.487l1.687-1.688a1.875 1.875 0 112.652 2.652L10.582 16.07a4.5 4.5 0 01-1.897 1.13L6 18l.8-2.685a4.5 4.5 0 011.13-1.897l8.932-8.931zm0 0L19.5 7.125M18 14v4.75A2.25 2.25 0 0115.75 21H5.25A2.25 2.25 0 013 18.75V8.25A2.25 2.25 0 015.25 6H10" />
								</svg>
								<span class="text-sm font-semibold {accessMode === 'read-write' ? 'text-accent-400' : 'text-slate-300'}">Read + Write</span>
							</div>
							<span class="text-xs text-slate-500">INSERT, UPDATE, DELETE enabled. Use for development databases.</span>
						</button>
					</div>
				</div>

				<div class="space-y-3">
					<h3 class="text-sm font-medium text-slate-300">Table Access</h3>
					<label class="flex items-center gap-3 cursor-pointer group">
						<div class="relative">
							<input type="checkbox" bind:checked={allTables} class="peer sr-only" />
							<div class="w-9 h-5 rounded-full bg-surface-700 peer-checked:bg-accent-500 transition-colors"></div>
							<div class="absolute top-0.5 left-0.5 w-4 h-4 rounded-full bg-white shadow-sm transition-transform peer-checked:translate-x-4"></div>
						</div>
						<span class="text-sm text-slate-400 group-hover:text-slate-300">Expose all tables</span>
					</label>
					{#if !allTables}
						<p class="text-xs text-slate-500 p-3 bg-surface-800/50 rounded-lg border border-surface-700/30">
							Table selection will be available after connecting. You can modify this later from the Sources page.
						</p>
					{/if}
				</div>
			</div>

		<!-- Step 4: Security -->
		{:else if currentStep === 3}
			<div class="space-y-6">
				<p class="text-sm text-slate-400">Choose how to secure your MCP endpoint</p>

				<div class="space-y-3">
					{#each [
						{ id: 'none', label: 'No Authentication', desc: 'Suitable for local development only', icon: '🔓' },
						{ id: 'api-key', label: 'API Key', desc: 'Generate a key for your AI clients', icon: '🔑' },
						{ id: 'bearer', label: 'Bearer Token', desc: 'Use an existing token for authentication', icon: '🛡️' }
					] as opt}
						<button
							onclick={() => { authMode = opt.id as typeof authMode; }}
							class="flex items-center gap-4 w-full p-4 rounded-xl border-2 text-left transition-all
								{authMode === opt.id
									? 'border-accent-500 bg-accent-500/5'
									: 'border-surface-700/50 bg-surface-900 hover:border-surface-600'}"
						>
							<span class="text-2xl">{opt.icon}</span>
							<div>
								<div class="text-sm font-semibold {authMode === opt.id ? 'text-accent-400' : 'text-slate-300'}">{opt.label}</div>
								<div class="text-xs text-slate-500 mt-0.5">{opt.desc}</div>
							</div>
						</button>
					{/each}
				</div>

				{#if authMode === 'api-key'}
					<div class="space-y-3">
						{#if generatedKey}
							<div class="flex items-center gap-2 p-3 bg-surface-800/80 rounded-lg border border-surface-700/30">
								<code class="flex-1 text-sm font-mono text-amber-300/90 break-all">{generatedKey}</code>
								<CopyButton text={generatedKey} />
							</div>
							<p class="text-xs text-amber-400/70">Save this key now. It won't be shown again.</p>
						{:else}
							<button
								onclick={generateApiKey}
								class="inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-accent-500 text-surface-950 font-medium text-sm hover:bg-accent-400 transition-colors"
							>
								Generate API Key
							</button>
						{/if}
					</div>
				{/if}
			</div>

		<!-- Step 5: Connect AI -->
		{:else if currentStep === 4}
			<div class="space-y-5">
				<p class="text-sm text-slate-400">Copy the configuration for your AI client</p>

				<div class="flex gap-1 p-1 bg-surface-800/50 rounded-lg border border-surface-700/30">
					{#each [
						{ id: 'claude-code', label: 'Claude Code' },
						{ id: 'cursor', label: 'Cursor' },
						{ id: 'vscode', label: 'VS Code' },
						{ id: 'claude-desktop', label: 'Claude Desktop' }
					] as tab}
						<button
							onclick={() => (selectedClient = tab.id as typeof selectedClient)}
							class="flex-1 px-3 py-2 rounded-md text-sm font-medium transition-all
								{selectedClient === tab.id
									? 'bg-surface-700 text-accent-400 shadow-sm'
									: 'text-slate-500 hover:text-slate-300'}"
						>
							{tab.label}
						</button>
					{/each}
				</div>

				<CodeBlock
					code={clientSnippets[selectedClient]}
					title={selectedClient === 'claude-code' ? 'Terminal' : 'JSON Configuration'}
					language={selectedClient === 'claude-code' ? 'bash' : 'json'}
				/>

				{#if selectedClient === 'claude-code'}
					<p class="text-xs text-slate-500">Run this command in your terminal to add Conduit as an MCP server.</p>
				{:else}
					<p class="text-xs text-slate-500">Add this to your {selectedClient === 'cursor' ? 'Cursor' : selectedClient === 'vscode' ? 'VS Code' : 'Claude Desktop'} settings file.</p>
				{/if}
			</div>
		{/if}
	</StepWizard>

	<!-- Navigation buttons -->
	<div class="flex items-center justify-between pt-4 border-t border-surface-700/30">
		<button
			onclick={prev}
			disabled={currentStep === 0}
			class="inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-slate-400 hover:text-slate-200 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
		>
			<svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
				<path stroke-linecap="round" stroke-linejoin="round" d="M15.75 19.5L8.25 12l7.5-7.5" />
			</svg>
			Back
		</button>

		{#if currentStep === steps.length - 1}
			<button
				onclick={handleFinish}
				class="inline-flex items-center gap-2 px-6 py-2.5 rounded-lg bg-accent-500 text-surface-950 font-semibold text-sm hover:bg-accent-400 transition-colors"
			>
				Finish Setup
				<svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2.5">
					<path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" />
				</svg>
			</button>
		{:else}
			<button
				onclick={next}
				disabled={!canProceed}
				class="inline-flex items-center gap-2 px-6 py-2.5 rounded-lg bg-accent-500 text-surface-950 font-semibold text-sm hover:bg-accent-400 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
			>
				Continue
				<svg class="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
					<path stroke-linecap="round" stroke-linejoin="round" d="M8.25 4.5l7.5 7.5-7.5 7.5" />
				</svg>
			</button>
		{/if}
	</div>
</div>
