import { parseFilter } from "./filterParser";




export function buildSql(queryString, { table = 'my_table'} = {} ) {
    const params = [];
    const query = Object.fromEntries(new URLSearchParams(queryString));

    // $select
    const columns = query['$select']
        ? query['$select'].split(',').map(s => s.trim()).join(', ')
        : '*';

    // $filter
    let where = '';
    if (query['$filter']) {
        const parsed = parseFilter(query['$filter']);
        where = `WHERE ${parsed.sql}`;
        params.push(...parsed.params);
    }

    // $orderBy
    const orderBy = query['$orderby']
        ? "ORDER BY " + query['$orderby']
        : '';
    
    // $top
    const limit = query['$top']
        ? `LIMIT ${parseInt(query["$top"])}`
        : '';

    // $skip
    const offset = query['$skip']
        ? `OFFSET ${parseInt(query['$skip'], 10)}`
        : '';
    
    const sql = `SELECT ${columns} FROM ${table} ${where} ${orderBy} ${limit} ${offset}`.trim();
    return { sql, params };
}