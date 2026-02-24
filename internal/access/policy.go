package access

// DefaultReadOnlyRole returns a role that allows SELECT on all tables.
func DefaultReadOnlyRole() Role {
	return Role{
		Name:            "readonly",
		MaxRowsPerQuery: 1000,
		Tables: []TablePolicy{
			{
				Name:  "*",
				Verbs: []string{"SELECT"},
			},
		},
	}
}

// DefaultAdminRole returns a role with full access to all tables.
func DefaultAdminRole() Role {
	return Role{
		Name:            "admin",
		MaxRowsPerQuery: 10000,
		Tables: []TablePolicy{
			{
				Name:  "*",
				Verbs: []string{"SELECT", "INSERT", "UPDATE", "DELETE"},
			},
		},
	}
}
