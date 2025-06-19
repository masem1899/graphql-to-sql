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
});
test('builds query with $expand for related table', () => {
    const query = "$select=name&$expand=roles($select=label)";
    const { sql, params } = buildSQL(query, {
        table: 'users',
        schema
    });

    expect(sql).toContain("LEFT JOIN roles r ON u.role_id = r.id");
    expect(sql).toContain("SELECT u.name, r.label");
    expect(sql).toContain("FROM users u");
    expect(params).toEqual([]);
});
test('combines $select and $expand with filters', () => {
    const query = "$select=name&$expand=roles($select=label)&$filter=label eq 'admin'";
    const { sql, params } = buildSQL(query, {
        table: 'users',
        schema
    });

    expect(sql).toContain("SELECT u.name, r.label");
    expect(sql).toContain("LEFT JOIN roles r ON u.role_id = r.id");
    expect(sql).toContain("WHERE r.label = ?");
    expect(params).toEqual(['admin']);
});
test('$apply generates GROUP BY + aggregate SQL', () => {
    const query = "$apply=groupby((city), aggregate(age with average as AvgAge))";

    const { sql, params } = buildSQL(query, {
        table: 'users',
        schema: {
            users: {
                alias: 'u',
                columns: ['id', 'name', 'age', 'city']
            }
        }
    });

    expect(sql).toBe("SELECT u.city, AVG(u.age) AS AvgAge FROM users u GROUP BY u.city");
    expect(params).toEqual([]);
});
test('$apply with $filter produces filtered groupby query', () => {
    const query = "$apply=groupby((city), aggregate(age with average as avgAge))&$filter=country eq 'Austria'";

    const { sql, params } = buildSQL(query, {
        table: 'users',
        schema: {
            users: {
                alias: 'u',
                columns: ['id', 'name', 'age', 'city']
            }
        }
    });

    expect(sql).toBe("SELECT u.city, AVG(u.age) AS avgAge FROM users u WHERE u.country = ? GROUP BY u.city");
    expect(params).toEqual(['Austria']);
});
test('supports postgres-style placeholders', () => {
    const query = "$filter=age gt 30 and city eq 'Vienna'";

    const { sql, params } = buildSQL(query, {
        table: 'users',
        placeholderStyle: 'postgres',
        schema: {
            users: {
                alias: 'u',
                columns: ['age', 'city']
            }
        }
    });

    expect(sql).toBe("SELECT * FROM users u WHERE (u.age > $1 AND u.city = $2)");
    expect(params).toEqual([30, 'Vienna']);
});