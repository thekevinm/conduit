import type { HealthResponse, Source, StatsResponse, ActivityEntry } from '$lib/api';

interface AppState {
	health: HealthResponse | null;
	sources: Source[];
	stats: StatsResponse | null;
	activity: ActivityEntry[];
	sidebarCollapsed: boolean;
	loading: boolean;
	error: string | null;
}

function createAppState() {
	let state = $state<AppState>({
		health: null,
		sources: [],
		stats: null,
		activity: [],
		sidebarCollapsed: false,
		loading: false,
		error: null
	});

	return {
		get health() { return state.health; },
		set health(v) { state.health = v; },
		get sources() { return state.sources; },
		set sources(v) { state.sources = v; },
		get stats() { return state.stats; },
		set stats(v) { state.stats = v; },
		get activity() { return state.activity; },
		set activity(v) { state.activity = v; },
		get sidebarCollapsed() { return state.sidebarCollapsed; },
		set sidebarCollapsed(v) { state.sidebarCollapsed = v; },
		get loading() { return state.loading; },
		set loading(v) { state.loading = v; },
		get error() { return state.error; },
		set error(v) { state.error = v; },

		get isConnected() {
			return state.health?.status === 'ok';
		},
		get sourceCount() {
			return state.sources.length;
		},
		toggleSidebar() {
			state.sidebarCollapsed = !state.sidebarCollapsed;
		}
	};
}

export const app = createAppState();
