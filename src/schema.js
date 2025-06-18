export const schema = {
    users: {
        alias: 'u',
        columns: ['id', 'name', 'email', 'role_id'],
        joins: {
            roles: {
                type: 'LEFT',
                on: 'u.role_id = r.id',
                alias: 'r'
            }
        }
    },
    roles: {
        alias: 'r',
        columns: ['id', 'label']
    }
};