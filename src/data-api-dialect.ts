import { Driver } from "kysely";
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
  #config: DataApiDialectConfig;

  constructor(config: DataApiDialectConfig) {
    this.#config = config;
  }

  createDriver(): Driver {
    return new DataApiDriver(this.#config.driver);
  }

  createQueryCompiler(): QueryCompiler {
    // The default query compiler is for postgres dialect.
    return new DataApiQueryCompiler();
  }

  createIntrospector(db: Kysely<any>): DatabaseIntrospector {
    console.log("Introspector");
    return new PostgresIntrospector(db);
  }
}
