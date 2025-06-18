const operators = {
    eq: '=',
    ne: '!=',
    gt: '>',
    ge: '>=',
    lt: '<',
    le: '<=',
};

const logical = {
    and: 'AND',
    or: 'OR',
};

function tokenize(input) {
    const regex = /\s*(=>|[()])|\b(?:and|or|eq|ne|gt|ge|lt|le)\b|('[^']*'|[a-zA-Z_][a-zA-Z0-9_]*|\d+(\.\d+)?)/gi;
    const tokens = [];
    let match;
    
    while ((match = regex.exec(input)) !== null) {
        const [token] = match;
        if (token.trim()) tokens.push(token.trim());
    }

    return tokens;
}

function parseExpression(tokens) {
    let { sql, params } = parseTerm(tokens);

    while (tokens[0] && logical[tokens[0].toLowerCase()]) {
        const op = logical[tokens.shift().toLowerCase()];
        const right = parseTerm(tokens);
        sql = `(${sql} ${op} ${right.sql})`;
        params.push(...right.params);
    }

    return { sql, params };
}

function parseTerm(tokens) {
    if (tokens[0] === '(') {
        tokens.shift();
        const expr = parseExpression(tokens);
        if (tokens.shift() !== ')') throw new Error("Expected ')'");
        return expr;
    }

    const field = tokens.shift();
    const opToken = tokens.shift()?.toLowerCase();
    const op = operators[opToken];
    if (!op) throw new Error(`Unknown operator: ${opToken}`);

    const valueToken = tokens.shift();
    if (!valueToken) throw new Error('Missing value');

    const value = valueToken.startsWith("'")
        ? valueToken.slice(1, -1)
        : parseFloat(valueToken);

    return { sql: `${field} ${op} ?`, params: [value] };
}

export function parseFilter(filterStr) {
    const tokens = tokenize(filterStr);
    const { sql, params } = parseExpression(tokens);
    if (tokens.length) throw new Error('Unexpected tokens: ' + tokens.join(' '));
    return { sql, params };
}