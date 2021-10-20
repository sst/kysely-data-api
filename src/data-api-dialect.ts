import { Driver, PostgresAdapter } from "kysely";
import { Kysely } from "kysely";
import { QueryCompiler } from "kysely";
import { Dialect } from "kysely";
import { DatabaseIntrospector } from "kysely";
import { PostgresIntrospector } from "kysely";
import { DataApiDriver, DataApiDriverConfig } from "./data-api-driver";
import { DataApiQueryCompiler } from "./data-api-query-compiler";

type DataApiDialectConfig = {
  driver: DataApiDriverConfig;
};

export class DataApiDialect implements Dialect {
  readonly #config: DataApiDialectConfig;

  constructor(config: DataApiDialectConfig) {
    this.#config = config;
  }

  createAdapter() {
    return new PostgresAdapter();
  }

  createDriver(): Driver {
    return new DataApiDriver(this.#config.driver);
  }

  createQueryCompiler(): QueryCompiler {
    // The default query compiler is for postgres dialect.
    return new DataApiQueryCompiler();
  }

  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    return new PostgresIntrospector(db);
  }
}
