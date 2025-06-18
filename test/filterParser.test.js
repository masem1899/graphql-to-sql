import { parseFilter } from '../src/filterParser';




test('parse simple filter', () => {
    const { sql, params} = parseFilter('age gt 30');
    expect(sql).toBe('age > ?');
    expect(params).toEqual([30]);
});
test('parses string values', () => {
    const { sql, params} = parseFilter("name eq 'Mario'");
    expect(sql).toBe('name = ?');
    expect(params).toEqual(['Mario']);
});
test('handles nested logic', () => {
    const { sql, params } = parseFilter("(age gt 20 or age lt 10) and name eq 'Mario'");
    expect(sql).toBe("((age > ? OR age < ?) AND name = ?)");
    expect(params).toEqual([20, 10, 'Mario']);
});
test('throws on invalid syntax', () => {
    expect(() => parseFilter("age between 20 and 30")).toThrow();
});