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

function parseExpandParam(expandStr) {
    const expansions = [];

    expandStr.split(',').forEach(exp => {
        const match = exp.trim().match(/^([a-zA-Z0-9_]+)(\(\$select=([^)]+)\))?$/);
        if (match) {
            const [, table, , selects] = match;
            expansions.push({
                table,
                selects: selects ? selects.split(',').map(s => s.trim()) : []
            });
        }
    });

    return expansions;
}

function parseApplyParam(applyStr) {
    const match = applyStr.match(/^groupby\(\(([^)]+)\),\s*aggregate\(([^)]+)\)\)$/);
    if (!match) return null;

    const groupByFields = match[1].split(',').map(f => f.trim());
    const aggregateDefs = match[2].split(',').map(def => {
        const aggMatch = def.trim().match(/^([a-zA-Z0-9_]+) with ([a-zA-Z]+) as ([a-zA-Z0-9_]+)$/);
        if (!aggMatch) return null;
        const [, field, func, alias] = aggMatch;
        return { field, func, alias};
    }).filter(Boolean);

    return { groupByFields, aggregateDefs };
}

export function buildSQL(input, { table = 'my_table', alias = null, schema = {}, placeholderStyle = 'mysql' } = {} ) {
    const query = createQuery(input);
    if (!query) {
        throw new Error('Unsupported input type for buildSql');
    }

    const params = [];

    const tableConfig = schema[table] || {};
    const baseAlias = alias || tableConfig.alias || null;
    const prefix = baseAlias ? `${baseAlias}.` : '';

    // $expand
    const expand = query.get('$expand')
        ? parseExpandParam(query.get('$expand'))
        : [];
    const expandedFields = [];
    const forcedJoins = new Set();

    for (const e of expand) {
        const joinInfo = schema[table]?.joins?.[e.table];
        if (!joinInfo) continue; // skip unknown joins

        forcedJoins.add(e.table);
        const alias = joinInfo.alias || e.table;

        if (e.selects.length > 0) {
            expandedFields.push(...e.selects.map(col => `${alias}.${col}`));
        }
    }

    // $apply
    const apply = query.get('$apply');
    let columns = '';
    let groupByClause = '';

    if(apply) {
        const parsedApply = parseApplyParam(apply);
        if (!parsedApply) throw new Error('Invalid $apply syntax');

        const { groupByFields, aggregateDefs } = parsedApply;

        const selectCols = [
            ...groupByFields.map(f => `${prefix}${f}`),
            ...aggregateDefs.map(({ field, func, alias }) => {
                const sqlFunc = {
                    average: 'AVG',
                    sum: 'SUM',
                    count: 'COUNT',
                    min: 'MIN',
                    max: 'MAX'
                }[func.toLowerCase()] || func.toUpperCase();
                return `${sqlFunc}(${prefix}${field}) AS ${alias}`;
            })
        ];

        columns = selectCols.join(', ');
        groupByClause = `GROUP BY ${groupByFields.map(f => `${prefix}${f}`).join(', ')}`;
        
    } else {
        // $select
        const selectFields = query.get('$select')
        ? query.get('$select').split(',').map(field => {
            for (const [tableName, def] of Object.entries(schema)) {
                if (def.columns?.includes(field.trim())) {
                    return `${def.alias || tableName}.${field.trim()}`;
                }
            }
            return prefix + field.trim();
        })
        : '*';
        
        columns = [...(Array.isArray(selectFields) ? selectFields : [selectFields]), ...expandedFields].join(', ');
    }


    // joins
    const joinClause = Object.entries(tableConfig.joins || {}).map(([joinedTable, def]) => {
        if (!forcedJoins.has(joinedTable) && !columns.includes(`${def.alias || joinedTable}.`)) {
            return null;
        }
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
    
    let sql = `SELECT ${columns} FROM ${table}${baseAlias ? ` ${baseAlias}` : ''} ${joinClause} ${where} ${groupByClause} ${orderBy} ${limit} ${offset}`
        .replace(/\s+/g, ' ')
        .trim();

    if (placeholderStyle === 'postgres') {
        let i = 1;
        sql = sql.replace(/\?/g, () => `$${i++}`);
    }

    return { sql, params };
}

export const buildSql = buildSQL; // deprecated alias