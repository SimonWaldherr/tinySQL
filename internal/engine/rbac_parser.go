// Parser and AST for RBAC DDL: CREATE/DROP/ALTER USER, CREATE/DROP ROLE,
// and GRANT/REVOKE. Enforcement lives in rbac.go; the underlying
// users/roles/grants model is internal/storage/rbac.go.
//
// Grammar (see parser_test-style examples in rbac_parser_test.go):
//
//	CREATE USER name WITH PASSWORD 'secret' [ROLE role1 [, role2 ...]]
//	DROP USER name
//	ALTER USER name ENABLE | DISABLE
//	ALTER USER name WITH PASSWORD 'newsecret'
//	CREATE ROLE name
//	DROP ROLE name
//	GRANT perm [, perm ...] ON [schema.]table TO ROLE role
//	GRANT perm [, perm ...] ON * TO ROLE role
//	REVOKE perm [, perm ...] ON [schema.]table FROM ROLE role
//	GRANT ROLE role TO USER user
//	REVOKE ROLE role FROM USER user
package engine

import "github.com/SimonWaldherr/tinySQL/internal/storage"

// CreateUser represents CREATE USER name WITH PASSWORD '...' [ROLE ...].
type CreateUser struct {
	Name     string
	Password string
	Roles    []string
}

// DropUser represents DROP USER name.
type DropUser struct {
	Name string
}

// AlterUser represents ALTER USER name {ENABLE|DISABLE|WITH PASSWORD '...'}.
// Exactly one of SetEnabled/NewPassword is set per statement (the grammar
// only allows one clause); both nil would be a no-op, which the parser
// never produces.
type AlterUser struct {
	Name        string
	SetEnabled  *bool
	NewPassword *string
}

// CreateRole represents CREATE ROLE name.
type CreateRole struct {
	Name string
}

// DropRole represents DROP ROLE name.
type DropRole struct {
	Name string
}

// GrantPrivilege represents GRANT perm[,...] ON target TO ROLE role.
type GrantPrivilege struct {
	Permissions []storage.Permission
	Schema      string // "*" for "every schema"
	Table       string // "*" for "every table"
	RoleName    string
}

// RevokePrivilege represents REVOKE perm[,...] ON target FROM ROLE role.
type RevokePrivilege struct {
	Permissions []storage.Permission
	Schema      string
	Table       string
	RoleName    string
}

// GrantRoleStmt represents GRANT ROLE role TO USER user.
type GrantRoleStmt struct {
	RoleName string
	UserName string
}

// RevokeRoleStmt represents REVOKE ROLE role FROM USER user.
type RevokeRoleStmt struct {
	RoleName string
	UserName string
}

// parseCreateUserOrRole dispatches CREATE USER / CREATE ROLE; called from
// parseCreateNonTable once the CREATE keyword has already been consumed.
func (p *Parser) parseCreateUserOrRole() (Statement, error) {
	switch p.cur.Val {
	case "USER":
		return p.parseCreateUser()
	case "ROLE":
		return p.parseCreateRole()
	}
	return nil, p.errf("expected USER or ROLE")
}

func (p *Parser) parseCreateUser() (Statement, error) {
	p.next() // consume USER
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected user name")
	}
	password := ""
	if p.cur.Typ == tKeyword && p.cur.Val == "WITH" {
		p.next()
		if err := p.expectKeyword("PASSWORD"); err != nil {
			return nil, err
		}
		pw, ok := p.parseStringLiteral()
		if !ok {
			return nil, p.errf("expected password string literal after WITH PASSWORD")
		}
		password = pw
	}
	if password == "" {
		return nil, p.errf("CREATE USER requires WITH PASSWORD '...'")
	}
	var roles []string
	if p.cur.Typ == tKeyword && p.cur.Val == "ROLE" {
		p.next()
		for {
			r := p.parseIdentLike()
			if r == "" {
				return nil, p.errf("expected role name")
			}
			roles = append(roles, r)
			if p.cur.Typ == tSymbol && p.cur.Val == "," {
				p.next()
				continue
			}
			break
		}
	}
	return &CreateUser{Name: name, Password: password, Roles: roles}, nil
}

func (p *Parser) parseCreateRole() (Statement, error) {
	p.next() // consume ROLE
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected role name")
	}
	return &CreateRole{Name: name}, nil
}

// parseDropUserOrRole is called from parseDrop once DROP has been consumed
// and the next token is USER or ROLE.
func (p *Parser) parseDropUserOrRole() (Statement, error) {
	switch p.cur.Val {
	case "USER":
		p.next()
		name := p.parseIdentLike()
		if name == "" {
			return nil, p.errf("expected user name")
		}
		return &DropUser{Name: name}, nil
	case "ROLE":
		p.next()
		name := p.parseIdentLike()
		if name == "" {
			return nil, p.errf("expected role name")
		}
		return &DropRole{Name: name}, nil
	}
	return nil, p.errf("expected USER or ROLE")
}

// parseAlterUser is called from parseAlter once ALTER USER has been
// consumed (the caller has already checked for the USER keyword).
func (p *Parser) parseAlterUser() (Statement, error) {
	p.next() // consume USER
	name := p.parseIdentLike()
	if name == "" {
		return nil, p.errf("expected user name")
	}
	switch {
	case p.cur.Typ == tKeyword && p.cur.Val == "ENABLE":
		p.next()
		v := false // "enabled" means Disabled=false
		return &AlterUser{Name: name, SetEnabled: &v}, nil
	case p.cur.Typ == tKeyword && p.cur.Val == "DISABLE":
		p.next()
		v := true
		return &AlterUser{Name: name, SetEnabled: &v}, nil
	case p.cur.Typ == tKeyword && p.cur.Val == "WITH":
		p.next()
		if err := p.expectKeyword("PASSWORD"); err != nil {
			return nil, err
		}
		pw, ok := p.parseStringLiteral()
		if !ok {
			return nil, p.errf("expected password string literal after WITH PASSWORD")
		}
		return &AlterUser{Name: name, NewPassword: &pw}, nil
	}
	return nil, p.errf("expected ENABLE, DISABLE, or WITH PASSWORD")
}

// parseGrantOrRevoke handles both GRANT and REVOKE, dispatching to the
// permission form (GRANT perm ON target TO ROLE r) or the role-membership
// form (GRANT ROLE r TO USER u) based on whether ROLE immediately follows
// the GRANT/REVOKE keyword.
func (p *Parser) parseGrantOrRevoke(isGrant bool) (Statement, error) {
	p.next() // consume GRANT/REVOKE

	if p.cur.Typ == tKeyword && p.cur.Val == "ROLE" {
		p.next()
		roleName := p.parseIdentLike()
		if roleName == "" {
			return nil, p.errf("expected role name")
		}
		if isGrant {
			if err := p.expectKeyword("TO"); err != nil {
				return nil, err
			}
		} else {
			if err := p.expectKeyword("FROM"); err != nil {
				return nil, err
			}
		}
		if err := p.expectKeyword("USER"); err != nil {
			return nil, err
		}
		userName := p.parseIdentLike()
		if userName == "" {
			return nil, p.errf("expected user name")
		}
		if isGrant {
			return &GrantRoleStmt{RoleName: roleName, UserName: userName}, nil
		}
		return &RevokeRoleStmt{RoleName: roleName, UserName: userName}, nil
	}

	var perms []storage.Permission
	for {
		name := p.parseIdentLike()
		if name == "" {
			return nil, p.errf("expected a permission (SELECT, INSERT, UPDATE, DELETE, DDL, or ALL)")
		}
		perm, err := storage.ParsePermission(name)
		if err != nil {
			return nil, p.errf("%s", err.Error())
		}
		perms = append(perms, perm)
		if p.cur.Typ == tSymbol && p.cur.Val == "," {
			p.next()
			continue
		}
		break
	}

	if err := p.expectKeyword("ON"); err != nil {
		return nil, err
	}
	schema, table, err := p.parseGrantTarget()
	if err != nil {
		return nil, err
	}

	if isGrant {
		if err := p.expectKeyword("TO"); err != nil {
			return nil, err
		}
	} else {
		if err := p.expectKeyword("FROM"); err != nil {
			return nil, err
		}
	}
	if err := p.expectKeyword("ROLE"); err != nil {
		return nil, err
	}
	roleName := p.parseIdentLike()
	if roleName == "" {
		return nil, p.errf("expected role name")
	}

	if isGrant {
		return &GrantPrivilege{Permissions: perms, Schema: schema, Table: table, RoleName: roleName}, nil
	}
	return &RevokePrivilege{Permissions: perms, Schema: schema, Table: table, RoleName: roleName}, nil
}

// parseGrantTarget parses the object a GRANT/REVOKE applies to: a bare "*"
// (every schema, every table), "schema.*" (every table in schema),
// "schema.table", or a bare "table" (schema defaults to "*", i.e. matches
// regardless of schema — most tinySQL tables live in the unqualified/
// default schema, so this keeps the common case simple).
func (p *Parser) parseGrantTarget() (schema, table string, err error) {
	if p.cur.Typ == tSymbol && p.cur.Val == "*" {
		p.next()
		return "*", "*", nil
	}
	first := p.parseIdentLike()
	if first == "" {
		return "", "", p.errf("expected a table name or '*' after ON")
	}
	if p.cur.Typ == tSymbol && p.cur.Val == "." {
		p.next()
		if p.cur.Typ == tSymbol && p.cur.Val == "*" {
			p.next()
			return first, "*", nil
		}
		second := p.parseIdentLike()
		if second == "" {
			return "", "", p.errf("expected a table name after '.'")
		}
		return first, second, nil
	}
	return "*", first, nil
}

// parseStringLiteral consumes a string literal token and returns
// (value, true), or ("", false) if the current token isn't a string.
func (p *Parser) parseStringLiteral() (string, bool) {
	if p.cur.Typ != tString {
		return "", false
	}
	v := p.cur.Val
	p.next()
	return v, true
}
