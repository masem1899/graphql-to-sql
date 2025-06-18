# graphql-to-sql

Convert MS Graph-style query strings like `$filter`, `$select`, `$orderby`, `$top` into SQL — safely and easily.

```js
import { buildSQL } from 'graphql-to-sql';

const query = "$select=name,age&$filter=age gt 20 and name eq 'Mario'&$orderby=age desc";
const { sql, params } = buildSQL(query, { table: 'users' });

console.log(sql);    // SELECT name, age FROM users WHERE age > ? AND name = ? ORDER BY age desc
console.log(params); // [20, 'Mario']
```

## Features
✅ Safe parameterized SQL

✅ Supports $filter, $select, $orderby, $top, $skip

🔜 Upcoming: joins, aliases, and $expand

## License
MIT

## Links
(Github Repository)[https://github.com/masem1899/graphql-to-sql]