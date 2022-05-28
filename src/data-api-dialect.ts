import {
  Driver,
  MysqlAdapter,
  MysqlIntrospector,
  PostgresAdapter,
  Kysely,
  QueryCompiler,
  Dialect,
  DatabaseIntrospector,
  PostgresIntrospector,
} from "kysely";
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
    if (this.#config.mode === "postgres") return new PostgresAdapter();
    if (this.#config.mode === "mysql") return new MysqlAdapter();

    throw new Error("Unknown mode " + this.#config.mode);
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

  createIntrospector(db: Kysely<unknown>): DatabaseIntrospector {
    if (this.#config.mode === "postgres") return new PostgresIntrospector(db);
    if (this.#config.mode === "mysql") return new MysqlIntrospector(db);

    throw new Error("Unknown mode " + this.#config.mode);
  }
}
