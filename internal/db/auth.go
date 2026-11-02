package db

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/cassandra-room/cassandra-room/internal/cql"
	"github.com/cassandra-room/cassandra-room/internal/types"
	"github.com/datastax/go-cassandra-native-protocol/message"
	"golang.org/x/crypto/bcrypt"
)

// Authentication and authorization, as Cassandra's PasswordAuthenticator,
// CassandraRoleManager and CassandraAuthorizer: roles with bcrypt password
// hashes, role membership, and permissions on resources (all keyspaces, a
// keyspace, a table; all roles, a role), inherited down the hierarchy and
// through granted roles. Roles are persisted with the schema.

// Role is a role (or user).
type Role struct {
	Name        string
	Hash        string `json:",omitempty"`
	Login       bool
	Superuser   bool
	MemberOf    []string            `json:",omitempty"`
	Permissions map[string][]string `json:",omitempty"` // resource -> permissions
}

// Permission names (Permission).
var allPermissions = map[string][]string{
	"data":  {"CREATE", "ALTER", "DROP", "SELECT", "MODIFY", "AUTHORIZE", "DESCRIBE", "UNMASK", "SELECT_MASKED"},
	"roles": {"CREATE", "ALTER", "DROP", "AUTHORIZE", "DESCRIBE"},
}

func applicable(resource string) []string {
	if strings.HasPrefix(resource, "roles") {
		return allPermissions["roles"]
	}
	return allPermissions["data"]
}

// Authenticate checks a login (PasswordAuthenticator.authenticate).
func (d *DB) Authenticate(user, password string) error {
	r := d.schema.Load().Roles[user]
	if r == nil || !r.Login || r.Hash == "" || bcrypt.CompareHashAndPassword([]byte(r.Hash), []byte(password)) != nil {
		return fmt.Errorf("Provided username %s and/or password are incorrect", user)
	}
	return nil
}

// AuthEnabled reports whether clients must log in.
func (d *DB) AuthEnabled() bool { return d.opts.Authenticator }

// defaultRoles creates the default superuser, cassandra/cassandra, when
// authentication is enabled and no role exists (CassandraRoleManager).
func (d *DB) defaultRoles() error {
	if !d.opts.Authenticator || len(d.schema.Load().Roles) > 0 {
		return nil
	}
	h, err := bcrypt.GenerateFromPassword([]byte("cassandra"), 10)
	if err != nil {
		return err
	}
	_, err = d.change("ROLES", func(next *Schema) (message.Message, bool, error) {
		if len(next.Roles) > 0 {
			return nil, false, nil
		}
		next.Roles["cassandra"] = &Role{Name: "cassandra", Hash: string(h), Login: true, Superuser: true}
		return nil, true, nil
	})
	return err
}

// roleClosure returns a role and the roles granted to it, transitively.
func roleClosure(roles map[string]*Role, name string) []*Role {
	var out []*Role
	seen := map[string]bool{}
	var walk func(string)
	walk = func(n string) {
		if seen[n] {
			return
		}
		seen[n] = true
		if r := roles[n]; r != nil {
			out = append(out, r)
			for _, m := range r.MemberOf {
				walk(m)
			}
		}
	}
	walk(name)
	return out
}

func isSuper(roles map[string]*Role, user string) bool {
	for _, r := range roleClosure(roles, user) {
		if r.Superuser {
			return true
		}
	}
	return false
}

// parents returns a resource and its ancestors.
func parents(resource string) []string {
	out := []string{resource}
	for {
		i := strings.LastIndexByte(resource, '/')
		if i < 0 {
			return out
		}
		resource = resource[:i]
		out = append(out, resource)
	}
}

// resourceName displays a resource (IResource.toString).
func resourceName(r string) string {
	parts := strings.Split(r, "/")
	switch {
	case r == "data":
		return "<all keyspaces>"
	case r == "roles":
		return "<all roles>"
	case parts[0] == "data" && len(parts) == 2:
		return "<keyspace " + parts[1] + ">"
	case parts[0] == "data":
		return "<table " + parts[1] + "." + parts[2] + ">"
	case parts[0] == "roles":
		return "<role " + parts[1] + ">"
	}
	return "<" + r + ">"
}

// authorize checks that user has perm on resource or a parent
// (ClientState.ensurePermission).
func (d *DB) authorize(user, perm, resource string) error {
	if !d.opts.Authorizer || user == "" {
		return nil
	}
	roles := d.schema.Load().Roles
	if isSuper(roles, user) {
		return nil
	}
	// Everyone may read the system keyspaces drivers use.
	if perm == "SELECT" && (strings.HasPrefix(resource, "data/system/") || strings.HasPrefix(resource, "data/system_schema/")) {
		return nil
	}
	for _, r := range roleClosure(roles, user) {
		for _, res := range parents(resource) {
			if slices.Contains(r.Permissions[res], perm) {
				return nil
			}
		}
	}
	e := invalidf("User %s has no %s permission on %s or any of its parents", user, perm, resourceName(resource)).(*requestError)
	e.kind = errUnauthorized
	return e
}

// required lists the permissions a prepared statement needs.
func (st *prepState) required(session string) [][2]string {
	data := func(ks, t string) string {
		if t == "" {
			return "data/" + ks
		}
		return "data/" + ks + "/" + t
	}
	switch s := st.stmt.(type) {
	case *selectStmt:
		return [][2]string{{"SELECT", data(s.t.Keyspace, s.t.Name)}}
	case *modification:
		return [][2]string{{"MODIFY", data(s.t.Keyspace, s.t.Name)}}
	case *batchStmt:
		var out [][2]string
		for _, m := range s.mods {
			out = append(out, [2]string{"MODIFY", data(m.t.Keyspace, m.t.Name)})
		}
		return out
	case *ddlStmt:
		ks := func(k string) string { k, _ = keyspaceOf(k, s.session); return k }
		switch r := s.raw.(type) {
		case *cql.CreateKeyspace:
			return [][2]string{{"CREATE", "data"}}
		case *cql.AlterKeyspace:
			return [][2]string{{"ALTER", data(r.Keyspace, "")}}
		case *cql.DropKeyspace:
			return [][2]string{{"DROP", data(r.Keyspace, "")}}
		case *cql.CreateTable:
			return [][2]string{{"CREATE", data(ks(r.Keyspace), "")}}
		case *cql.AlterTable:
			return [][2]string{{"ALTER", data(ks(r.Keyspace), r.Table)}}
		case *cql.DropTable:
			return [][2]string{{"DROP", data(ks(r.Keyspace), r.Table)}}
		case *cql.Truncate:
			return [][2]string{{"MODIFY", data(ks(r.Keyspace), r.Table)}}
		case *cql.CreateIndex:
			return [][2]string{{"ALTER", data(ks(r.Keyspace), r.Table)}}
		case *cql.DropIndex:
			return [][2]string{{"ALTER", data(ks(r.Keyspace), "")}}
		case *cql.CreateType:
			return [][2]string{{"CREATE", data(ks(r.Keyspace), "")}}
		case *cql.AlterType:
			return [][2]string{{"ALTER", data(ks(r.Keyspace), "")}}
		case *cql.DropType:
			return [][2]string{{"DROP", data(ks(r.Keyspace), "")}}
		}
	}
	return nil
}

// authStmt runs role, grant and list statements.
type authStmt struct {
	raw     cql.Statement
	session string
}

func cloneRoles(m map[string]*Role) map[string]*Role {
	out := make(map[string]*Role, len(m))
	for k, r := range m {
		c := *r
		c.MemberOf = slices.Clone(r.MemberOf)
		c.Permissions = maps.Clone(r.Permissions)
		out[k] = &c
	}
	return out
}

func unauthorized(format string, args ...any) error {
	e := invalidf(format, args...).(*requestError)
	e.kind = errUnauthorized
	return e
}

func (a *authStmt) execute(e *execCtx) (message.Message, error) {
	if l, ok := a.raw.(*cql.ListStmt); ok {
		roles := e.d.schema.Load().Roles
		user := e.user
		super := user == "" || isSuper(roles, user) || !e.d.opts.Authorizer
		return a.list(roles, l, user, super)
	}
	// Hash passwords once, outside the plan, which may run several times.
	var hash string
	if r, ok := a.raw.(*cql.RoleStmt); ok && r.Password != nil {
		h, err := bcrypt.GenerateFromPassword([]byte(*r.Password), 10)
		if err != nil {
			return nil, err
		}
		hash = string(h)
	}
	return e.d.change("ROLES", func(next *Schema) (message.Message, bool, error) { return a.plan(e, next, hash) })
}

// plan applies a role or permission change to the metadata.
func (a *authStmt) plan(e *execCtx, next *Schema, hash string) (res message.Message, changed bool, err error) {
	d := e.d
	roles := next.Roles
	user := e.user
	super := user == "" || isSuper(roles, user) || !d.opts.Authorizer
	need := func(perm, resource string) error {
		if super {
			return nil
		}
		return d.authorize(user, perm, resource)
	}
	switch s := a.raw.(type) {
	case *cql.RoleStmt:
		r := roles[s.Name]
		switch s.Op {
		case "CREATE":
			if err := need("CREATE", "roles"); err != nil {
				return nil, false, err
			}
			if r != nil {
				if s.IfNotExists {
					return &message.VoidResult{}, false, nil
				}
				return nil, false, invalidf("%s %s already exists", map[bool]string{true: "User", false: "Role"}[s.User], s.Name)
			}
			if s.Superuser != nil && *s.Superuser && !super {
				return nil, false, unauthorized("Only superusers can create a role with superuser status")
			}
			r = &Role{Name: s.Name}
			roles[s.Name] = r
			if user != "" && !super {
				// The creator may manage the role (CassandraRoleManager).
				roles[user].Permissions = withPerms(roles[user].Permissions, "roles/"+s.Name, applicable("roles"))
			}
		case "ALTER":
			if r == nil {
				return nil, false, invalidf("%s doesn't exist", s.Name)
			}
			self := s.Name == user && s.Superuser == nil && s.Login == nil
			if !self {
				if err := need("ALTER", "roles/"+s.Name); err != nil {
					return nil, false, err
				}
			}
			if s.Superuser != nil && !super {
				return nil, false, unauthorized("Only superusers are allowed to alter superuser status")
			}
		case "DROP":
			if r == nil {
				if s.IfExists {
					return &message.VoidResult{}, false, nil
				}
				return nil, false, invalidf("%s doesn't exist", s.Name)
			}
			if err := need("DROP", "roles/"+s.Name); err != nil {
				return nil, false, err
			}
			if s.Name == user {
				return nil, false, invalidf("Cannot DROP primary role for current login")
			}
			delete(roles, s.Name)
			for _, o := range roles {
				o.MemberOf = slices.DeleteFunc(o.MemberOf, func(m string) bool { return m == s.Name })
			}
			return &message.VoidResult{}, true, nil
		}
		if s.Password != nil {
			r.Hash = hash
		}
		if s.Login != nil {
			r.Login = *s.Login
		}
		if s.Superuser != nil {
			r.Superuser = *s.Superuser
		}
		return &message.VoidResult{}, true, nil
	case *cql.GrantStmt:
		grantee := roles[s.Grantee]
		if grantee == nil {
			return nil, false, invalidf("%s doesn't exist", s.Grantee)
		}
		if s.Role != "" {
			if roles[s.Role] == nil {
				return nil, false, invalidf("%s doesn't exist", s.Role)
			}
			if err := need("AUTHORIZE", "roles/"+s.Role); err != nil {
				return nil, false, err
			}
			if s.Revoke {
				grantee.MemberOf = slices.DeleteFunc(grantee.MemberOf, func(m string) bool { return m == s.Role })
			} else {
				for _, r := range roleClosure(roles, s.Role) {
					if r.Name == s.Grantee {
						return nil, false, invalidf("%s is a member of %s", s.Role, s.Grantee)
					}
				}
				if !slices.Contains(grantee.MemberOf, s.Role) {
					grantee.MemberOf = append(grantee.MemberOf, s.Role)
				}
			}
			return &message.VoidResult{}, true, nil
		}
		res := a.resource(s.Resource)
		if err := d.checkResource(res); err != nil {
			return nil, false, err
		}
		if err := need("AUTHORIZE", res); err != nil {
			return nil, false, err
		}
		perms := s.Permissions
		if perms[0] == "ALL" {
			perms = applicable(res)
		}
		for _, p := range perms {
			if !slices.Contains(applicable(res), p) {
				return nil, false, invalidf("Resource type %s does not support any of the requested permissions", strings.SplitN(res, "/", 2)[0])
			}
		}
		if s.Revoke {
			left := slices.DeleteFunc(slices.Clone(grantee.Permissions[res]), func(p string) bool { return slices.Contains(perms, p) })
			if grantee.Permissions == nil {
				grantee.Permissions = map[string][]string{}
			}
			if len(left) == 0 {
				delete(grantee.Permissions, res)
			} else {
				grantee.Permissions[res] = left
			}
		} else {
			grantee.Permissions = withPerms(grantee.Permissions, res, perms)
		}
		return &message.VoidResult{}, true, nil
	}
	return nil, false, invalidf("unsupported statement")
}

func withPerms(m map[string][]string, res string, perms []string) map[string][]string {
	if m == nil {
		m = map[string][]string{}
	}
	cur := slices.Clone(m[res])
	for _, p := range perms {
		if !slices.Contains(cur, p) {
			cur = append(cur, p)
		}
	}
	m[res] = cur
	return m
}

// resource completes a table resource with the session's keyspace.
func (a *authStmt) resource(r string) string {
	if strings.HasPrefix(r, "data//") {
		return "data/" + a.session + r[len("data/"):]
	}
	return r
}

func (d *DB) checkResource(res string) error {
	parts := strings.Split(res, "/")
	if parts[0] != "data" || len(parts) == 1 {
		return nil
	}
	s := d.schema.Load()
	k, ok := s.Keyspaces[parts[1]]
	if !ok {
		return invalidf("Resource %s doesn't exist", resourceName(res))
	}
	if len(parts) == 3 && k.Tables[parts[2]] == nil {
		return invalidf("Resource %s doesn't exist", resourceName(res))
	}
	return nil
}

func (a *authStmt) list(roles map[string]*Role, s *cql.ListStmt, user string, super bool) (message.Message, error) {
	text, boolean := types.TextType.DataType(), types.BooleanType.DataType()
	md := &message.RowsMetadata{}
	col := func(name string, isBool bool) {
		t := text
		if isBool {
			t = boolean
		}
		md.Columns = append(md.Columns, &message.ColumnMetadata{Keyspace: "system_auth", Table: "roles", Name: name, Index: int32(len(md.Columns)), Type: t})
	}
	b := func(v bool) []byte { return []byte{boolByte(v)} }
	var names []string
	if s.Of != "" {
		if roles[s.Of] == nil {
			return nil, invalidf("%s doesn't exist", s.Of)
		}
		if !super && s.Of != user {
			return nil, unauthorized("You are not authorized to view roles granted to %s", s.Of)
		}
		if s.NoRecursive {
			names = []string{s.Of}
		} else {
			for _, r := range roleClosure(roles, s.Of) {
				names = append(names, r.Name)
			}
		}
	} else {
		for n := range roles {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	var rows message.RowSet
	switch s.What {
	case "ROLES":
		col("role", false)
		col("super", true)
		col("login", true)
		for _, n := range names {
			r := roles[n]
			rows = append(rows, message.Row{[]byte(n), b(r.Superuser), b(r.Login)})
		}
	case "USERS":
		col("name", false)
		col("super", true)
		for _, n := range names {
			if r := roles[n]; r.Login {
				rows = append(rows, message.Row{[]byte(n), b(r.Superuser)})
			}
		}
	case "PERMISSIONS":
		col("role", false)
		col("username", false)
		col("resource", false)
		col("permission", false)
		res := a.resource(s.Resource)
		for _, n := range names {
			r := roles[n]
			var resources []string
			for k := range r.Permissions {
				if res == "" || slices.Contains(parents(res), k) {
					resources = append(resources, k)
				}
			}
			sort.Strings(resources)
			for _, k := range resources {
				for _, p := range r.Permissions[k] {
					if s.Permissions[0] != "ALL" && !slices.Contains(s.Permissions, p) {
						continue
					}
					rows = append(rows, message.Row{[]byte(n), []byte(n), []byte(resourceName(k)), []byte(p)})
				}
			}
		}
	}
	md.ColumnCount = int32(len(md.Columns))
	if rows == nil {
		rows = message.RowSet{}
	}
	return &message.RowsResult{Metadata: md, Data: rows}, nil
}

var errNoLogin = errors.New("You have not logged in")
