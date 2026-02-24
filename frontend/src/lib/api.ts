const BASE = '/api/v1';

export interface HealthResponse {
	status: string;
	version: string;
	uptime: number;
	sources: number;
}

export interface Source {
	id: string;
	name: string;
	type: string;
	host: string;
	port: number;
	database: string;
	tables: number;
	status: 'connected' | 'disconnected' | 'error';
	access_mode: 'read-only' | 'read-write';
	created_at: string;
}

export interface TableInfo {
	name: string;
	schema: string;
	row_count: number;
	columns: ColumnInfo[];
	primary_key: string[];
	indexes: IndexInfo[];
	foreign_keys: ForeignKeyInfo[];
}

export interface ColumnInfo {
	name: string;
	type: string;
	nullable: boolean;
	default_value: string | null;
	is_primary_key: boolean;
	is_foreign_key: boolean;
}

export interface IndexInfo {
	name: string;
	columns: string[];
	unique: boolean;
}

export interface ForeignKeyInfo {
	column: string;
	references_table: string;
	references_column: string;
}

export interface StatsResponse {
	sources_connected: number;
	tables_exposed: number;
	queries_today: number;
	avg_latency_ms: number;
}

export interface ActivityEntry {
	id: string;
	tool: string;
	table: string;
	duration_ms: number;
	timestamp: string;
	status: 'success' | 'error';
	client: string;
}

export interface AuditEntry {
	id: string;
	timestamp: string;
	action: string;
	tool: string;
	table: string;
	client: string;
	duration_ms: number;
	status: 'success' | 'error';
	details: string;
}

export interface ApiKey {
	id: string;
	name: string;
	prefix: string;
	created_at: string;
	last_used: string | null;
	expires_at: string | null;
}

export interface EndpointInfo {
	url: string;
	transport: 'stdio' | 'http';
	status: 'active' | 'inactive';
	requests_today: number;
}

export interface ClientSnippet {
	client: string;
	language: string;
	snippet: string;
}

async function request<T>(path: string, options?: RequestInit): Promise<T> {
	const res = await fetch(`${BASE}${path}`, {
		headers: {
			'Content-Type': 'application/json',
			...options?.headers
		},
		...options
	});
	if (!res.ok) {
		const err = await res.json().catch(() => ({ error: res.statusText }));
		throw new Error(err.error || res.statusText);
	}
	return res.json();
}

export async function fetchHealth(): Promise<HealthResponse> {
	return request('/health');
}

export async function fetchSources(): Promise<Source[]> {
	return request('/sources');
}

export async function fetchSourceTables(sourceId: string): Promise<TableInfo[]> {
	return request(`/sources/${sourceId}/tables`);
}

export async function fetchTableDetail(sourceId: string, tableName: string): Promise<TableInfo> {
	return request(`/sources/${sourceId}/tables/${tableName}`);
}

export async function fetchStats(): Promise<StatsResponse> {
	return request('/stats');
}

export async function fetchActivity(): Promise<ActivityEntry[]> {
	return request('/activity');
}

export async function testConnection(config: Record<string, unknown>): Promise<{ success: boolean; message: string }> {
	return request('/sources/test', {
		method: 'POST',
		body: JSON.stringify(config)
	});
}

export async function addSource(config: Record<string, unknown>): Promise<Source> {
	return request('/sources', {
		method: 'POST',
		body: JSON.stringify(config)
	});
}

export async function removeSource(id: string): Promise<void> {
	await fetch(`${BASE}/sources/${id}`, { method: 'DELETE' });
}

export async function fetchAuditLog(): Promise<AuditEntry[]> {
	return request('/audit/recent');
}

export async function fetchApiKeys(): Promise<ApiKey[]> {
	return request('/auth/keys');
}

export async function generateApiKey(name: string): Promise<{ key: string; id: string }> {
	return request('/auth/keys', {
		method: 'POST',
		body: JSON.stringify({ name })
	});
}

export async function revokeApiKey(id: string): Promise<void> {
	await fetch(`${BASE}/auth/keys/${id}`, { method: 'DELETE' });
}

export async function fetchEndpoints(): Promise<EndpointInfo[]> {
	return request('/endpoints');
}

export async function fetchClientSnippet(client: string): Promise<ClientSnippet> {
	return request(`/clients/${client}/snippet`);
}
