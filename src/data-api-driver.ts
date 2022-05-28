import { DatabaseConnection, QueryResult } from "kysely";
import { Driver } from "kysely";
import { CompiledQuery } from "kysely";
import RDSDataService, { SqlParametersList } from "aws-sdk/clients/rdsdataservice";

export type DataApiDriverConfig = {
  client: RDSDataService;
  secretArn: string;
  resourceArn: string;
  database: string;
};

export class DataApiDriver implements Driver {
  #config: DataApiDriverConfig;

  constructor(config: DataApiDriverConfig) {
    this.#config = config;
  }

  async init(): Promise<void> {
    // do nothing
  }

  async acquireConnection(): Promise<DatabaseConnection> {
    return new DataApiConnection(this.#config);
  }

  async beginTransaction(conn: DataApiConnection) {
    await conn.beginTransaction();
  }

  async commitTransaction(conn: DataApiConnection) {
    await conn.commitTransaction();
  }

  async rollbackTransaction(conn: DataApiConnection) {
    await conn.rollbackTransaction();
  }

  async releaseConnection(_connection: DatabaseConnection): Promise<void> {
    // do nothing
  }

  async destroy(): Promise<void> {
    // do nothing
  }
}

class DataApiConnection implements DatabaseConnection {
  #config: DataApiDriverConfig;
  #transactionId?: string;

  constructor(config: DataApiDriverConfig) {
    this.#config = config;
  }

  public async beginTransaction() {
    const r = await this.#config.client
      .beginTransaction({
        secretArn: this.#config.secretArn,
        resourceArn: this.#config.resourceArn,
        database: this.#config.database,
      })
      .promise();
    this.#transactionId = r.transactionId;
  }

  public async commitTransaction() {
    if (!this.#transactionId)
      throw new Error("Cannot commit a transaction before creating it");
    await this.#config.client
      .commitTransaction({
        secretArn: this.#config.secretArn,
        resourceArn: this.#config.resourceArn,
        transactionId: this.#transactionId,
      })
      .promise();
  }

  public async rollbackTransaction() {
    if (!this.#transactionId)
      throw new Error("Cannot rollback a transaction before creating it");
    await this.#config.client
      .rollbackTransaction({
        secretArn: this.#config.secretArn,
        resourceArn: this.#config.resourceArn,
        transactionId: this.#transactionId,
      })
      .promise();
  }

  async executeQuery<O>(compiledQuery: CompiledQuery): Promise<QueryResult<O>> {
    const r = await this.#config.client
      .executeStatement({
        transactionId: this.#transactionId,
        secretArn: this.#config.secretArn,
        resourceArn: this.#config.resourceArn,
        sql: compiledQuery.sql,
        parameters: compiledQuery.parameters as SqlParametersList,
        database: this.#config.database,
        includeResultMetadata: true,
      })
      .promise();
    if (!r.columnMetadata) {
      return {
        numUpdatedOrDeletedRows: BigInt(r.numberOfRecordsUpdated || 0),
        rows: [],
      };
    }
    const rows = r.records
      ?.filter((r) => r.length !== 0)
      .map(
        (rec) =>
          Object.fromEntries(
            rec.map((val, i) => [
              r.columnMetadata?.[i].name,
              val.stringValue ??
                val.blobValue ??
                val.longValue ??
                val.arrayValue ??
                val.doubleValue ??
                (val.isNull ? null : val.booleanValue),
            ]),
          ) as unknown as O,
      );
    const result: QueryResult<O> = {
      rows: rows || [],
    };
    return result;
  }
}
