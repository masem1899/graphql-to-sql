import { buildSql } from "../src/sqlBuilder";




test('build full query', () => {
    const q = "$select=name,age&$filter=age gt 10&$orderby=age desc&$top=5&$skip=2";
    const { sql, params } = buildSql(q, { table:'users' });

    expect(sql).toBe("SELECT name, age FROM users WHERE age > ? ORDER BY age desc LIMIT 5 OFFSET 2");
    expect(params).toEqual([10]);
});