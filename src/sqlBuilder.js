import { parseFilter } from "./filterParser.js";




function createQuery(input /* string | URLSearchParams | RequestLike */) {
    if(typeof(input) === 'string') {
        return new URLSearchParams(input);
    } else if(typeof(input) instanceof URLSearchParams) {
        return input;
    } else if(input && input.url) {
        const url = new URL(input.url, 'http://localhost');
        return url.searchParams;
    }

    return null;
}

export function buildSQL(input, { table = 'my_table', alias = null} = {} ) {
    const query = createQuery(input);
    if (!query) {
        throw new Error('Unsupported input type for buildSql');
    }

    const params = [];

    // $select
    const columns = query.get('$select')
        ? query.get('$select').split(',').map(s => s.trim()).join(', ')
        : '*';

    // $filter
    let where = '';
    if (query.get('$filter')) {
        const parsed = parseFilter(query.get('$filter'));
        where = `WHERE ${parsed.sql}`;
        params.push(...parsed.params);
    }

    // $orderBy
    const orderBy = query.get('$orderby')
        ? "ORDER BY " + query.get('$orderby')
        : '';
    
    // $top
    const limit = query.get('$top')
        ? `LIMIT ${parseInt(query.get("$top"))}`
        : '';

    // $skip
    const offset = query.get('$skip')
        ? `OFFSET ${parseInt(query.get('$skip'), 10)}`
        : '';
    
    const sql = `SELECT ${columns} FROM ${table} ${where} ${orderBy} ${limit} ${offset}`.trim();
    return { sql, params };
}

export const buildSql = buildSQL; // deprecated alias