## Kysely Data Api

This library adds AWS RDS Data Api support for [kysely](https://github.com/koskimas/kysely). It has support for both MySQL and Postgres

### Usage

```typescript
const dataApi = new DataApiDialect({
  mode: "mysql",
  driver: {
    client: new RDSDataService(),
    database: "bench",
    secretArn: "<arn of secret containing credentials",
    resourceArn: "<arn of database>",
  },
});

export const db = new Kysely<Database>({ dialect: dataApi });
```
