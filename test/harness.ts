import RDSDataService from "aws-sdk/clients/rdsdataservice";
import { ColumnType, Generated, Kysely } from "kysely";
import { DataApiDialect } from "../src";
import { DataApiDriverConfig } from "../src/data-api-driver";
import path from "path";

const TEST_DATABASE = "scratch";

const opts: DataApiDriverConfig = {
  client: new RDSDataService(),
  database: TEST_DATABASE,
  secretArn: process.env.RDS_SECRET,
  resourceArn: process.env.RDS_ARN,
};
const dialect = new DataApiDialect({
  mode: "postgres",
  driver: opts,
});

export interface Person {
  avatar: Buffer;
  created_at: ColumnType<Date, never, never>;
  balance: ColumnType<string, number, number>;
  first_name: string;
  gender: "male" | "female" | "other";
  id: Generated<number>;
  is_active: boolean;
  last_name: string;
  score: bigint;
}

export interface Pet {
  id: Generated<number>;
  owner_id: number;
  name: string;
  species: "dog" | "cat";
}

// Keys are table names.
interface Database {
  person: Person;
  pet: Pet;
}

export const db = new Kysely<Database>({ dialect });

export async function migrate() {
  await opts.client
    .executeStatement({
      sql: `SELECT pg_terminate_backend(pid)
      FROM pg_stat_activity
      WHERE pid <> pg_backend_pid() AND datname = '${TEST_DATABASE}'`,
      database: "postgres",
      secretArn: opts.secretArn,
      resourceArn: opts.resourceArn,
    })
    .promise();

  await opts.client
    .executeStatement({
      sql: `DROP DATABASE IF EXISTS ${TEST_DATABASE}`,
      database: "postgres",
      secretArn: opts.secretArn,
      resourceArn: opts.resourceArn,
    })
    .promise();

  await opts.client
    .executeStatement({
      sql: `CREATE DATABASE ${TEST_DATABASE}`,
      database: "postgres",
      secretArn: opts.secretArn,
      resourceArn: opts.resourceArn,
    })
    .promise();

  await db.migration.migrateToLatest(path.resolve("./test/migrations"));
}

export async function reset() {
  await db.deleteFrom("person").execute();
  await db.deleteFrom("pet").execute();
}
