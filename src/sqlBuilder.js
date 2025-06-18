import { parseFilter } from "./filterParser.js";




// Reserved keywords, that should not be prefixed
const RESERVED = new Set(['and','or','not','eq','ne','gt','lt','ge','le','true','false','null']);


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

export function buildSQL(input, { table = 'my_table', alias = null, schema = {} } = {} ) {
    const query = createQuery(input);
    if (!query) {
        throw new Error('Unsupported input type for buildSql');
    }

    const params = [];

    const tableConfig = schema[table] || {};
    const baseAlias = alias || tableConfig.alias || null;
    const prefix = baseAlias ? `${baseAlias}.` : '';

    // $select
    const columns = query.get('$select')
        ? query.get('$select').split(',').map(field => {
            for (const [tableName, def] of Object.entries(schema)) {
                if (def.columns?.includes(field.trim())) {
                    return `${def.alias || tableName}.${field.trim()}`;
                }
            }
            return prefix + field.trim();
        }).join(', ')
        : '*';

    // joins
    const joins = tableConfig.joins || {};
    const joinClause = Object.entries(joins).map(([joinedTable, def]) => {
        return `${def.type || 'LEFT'} JOIN ${joinedTable} ${def.alias || joinedTable} ON ${def.on}`;
    }).join(' ');

    // $filter
    let where = '';
    if (query.get('$filter')) {
        const parsed = parseFilter(query.get('$filter'));
        // prefix fields with correct aliases
        parsed.sql = parsed.sql.replace(/\b([a-zA-Z_][a-zA-Z0-9_]*)\b/g, (match) => {
            if (RESERVED.has(match.toLowerCase())) return match;

            for (const [tableName, def] of Object.entries(schema)) {
                if (def.columns?.includes(match)) {
                    return `${def.alias || tableName}.${match}`;
                }
            }
            return prefix + match;
        });
        where = `WHERE ${parsed.sql}`;
        params.push(...parsed.params);
    }

    // $orderBy
    const orderBy = query.get('$orderby')
        ? "ORDER BY " + query.get('$orderby').split(',').map(o => {
            const [col, dir] = o.trim().split(/\s+/);
            for (const [tableName, def] of Object.entries(schema)) {
                if (def.columns?.includes(col)) {
                    return `${def.alias || tableName}.${col} ${dir || ''}`.trim();
                }
            }
            return `${prefix}${col} ${dir || ''}`.trim();
        }).join(', ')
        : '';
    
    // $top
    const limit = query.get('$top')
        ? `LIMIT ${parseInt(query.get("$top"))}`
        : '';

    // $skip
    const offset = query.get('$skip')
        ? `OFFSET ${parseInt(query.get('$skip'), 10)}`
        : '';
    
    const sql = `SELECT ${columns} FROM ${table}${baseAlias ? ` ${baseAlias}` : ''} ${joinClause} ${where} ${orderBy} ${limit} ${offset}`
        .replace(/\s+/g, ' ')
        .trim();
    return { sql, params };
}

export const buildSql = buildSQL; // deprecated alias