<script lang="ts">
	let {
		type,
		size = 'md',
		class: className = ''
	}: {
		type: string;
		size?: 'sm' | 'md' | 'lg' | 'xl';
		class?: string;
	} = $props();

	const sizeMap: Record<string, string> = {
		sm: 'w-6 h-6 text-xs',
		md: 'w-10 h-10 text-sm',
		lg: 'w-14 h-14 text-base',
		xl: 'w-20 h-20 text-lg'
	};

	const dbConfig: Record<string, { bg: string; text: string; label: string; icon: string }> = {
		postgresql: { bg: 'bg-blue-500/15', text: 'text-blue-400', label: 'PG', icon: '🐘' },
		postgres: { bg: 'bg-blue-500/15', text: 'text-blue-400', label: 'PG', icon: '🐘' },
		mysql: { bg: 'bg-orange-500/15', text: 'text-orange-400', label: 'My', icon: '🐬' },
		mssql: { bg: 'bg-red-500/15', text: 'text-red-400', label: 'MS', icon: '🔷' },
		sqlserver: { bg: 'bg-red-500/15', text: 'text-red-400', label: 'MS', icon: '🔷' },
		sqlite: { bg: 'bg-sky-500/15', text: 'text-sky-400', label: 'SL', icon: '🪶' },
		oracle: { bg: 'bg-rose-500/15', text: 'text-rose-400', label: 'Or', icon: '🔴' },
		snowflake: { bg: 'bg-cyan-500/15', text: 'text-cyan-400', label: 'SF', icon: '❄️' }
	};

	const config = $derived(dbConfig[type.toLowerCase()] ?? { bg: 'bg-surface-700/50', text: 'text-slate-400', label: '?', icon: '💾' });
</script>

<div
	class="inline-flex items-center justify-center rounded-lg {config.bg} {config.text} font-mono font-bold {sizeMap[size]} {className}"
	title={type}
>
	{config.icon}
</div>
