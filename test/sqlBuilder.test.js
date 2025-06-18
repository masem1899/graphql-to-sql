import { buildSQL } from "../src/sqlBuilder";
import { schema } from "../src/schema.js";





test('build full query', () => {
    const q = "$select=name,age&$filter=age gt 10&$orderby=age desc&$top=5&$skip=2";
    const { sql, params } = buildSQL(q, { table:'users' });

    expect(sql).toBe("SELECT name, age FROM users WHERE age > ? ORDER BY age desc LIMIT 5 OFFSET 2");
    expect(params).toEqual([10]);
});
test('builds query with table alias', () => {
    const query = "$select=name,age&$filter=age gt 18 and city eq 'Vienna'&$orderby=age desc";
    const { sql, params } = buildSQL(query, { table:'users', alias:'u' });

    expect(sql).toBe(
        "SELECT u.name, u.age FROM users u WHERE (u.age > ? AND u.city = ?) ORDER BY u.age desc"
    );
    expect(params).toEqual([18, 'Vienna']);
});
test('builds query with JOIN using schema', () => {
    const query = "$select=name,label&$filter=label eq 'admin'";
    const { sql, params } = buildSQL(query, {
        table:'users',
        schema
    });

    expect(sql).toContain('JOIN roles r ON u.role_id = r.id');
    expect(sql).toContain('SELECT u.name, r.label');
    expect(sql).toContain('WHERE r.label = ?');
    
    expect(params).toEqual(['admin']);
})