import { Driver, PostgresAdapter } from "kysely";
import { Kysely } from "kysely";
import { QueryCompiler } from "kysely";
import { Dialect } from "kysely";
import { DatabaseIntrospector } from "kysely";
import { PostgresIntrospector } from "kysely";
import { DataApiDriver, DataApiDriverConfig } from "./data-api-driver";
import {
  PostgresDataApiQueryCompiler,
  MysqlDataApiQueryCompiler,
} from "./data-api-query-compiler";

type DataApiDialectConfig = {
  mode: "postgres" | "mysql";
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
    if (this.#config.mode === "postgres")
      return new PostgresDataApiQueryCompiler();
    if (this.#config.mode === "mysql") return new MysqlDataApiQueryCompiler();

    throw new Error("Unknown mode " + this.#config.mode);
  }

  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    return new PostgresIntrospector(db);
  }
}
