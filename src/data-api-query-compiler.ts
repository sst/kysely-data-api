import { Field } from "aws-sdk/clients/rdsdataservice";
import { MysqlQueryCompiler, PostgresQueryCompiler } from "kysely";

export class PostgresDataApiQueryCompiler extends PostgresQueryCompiler {
  protected override appendValue(value: any) {
    const name = this.numParameters;
    this.append(this.getCurrentParameterPlaceholder());
    this.addParameter({
      name: name.toString(),
      value: serialize(value),
    });
  }

  protected override getCurrentParameterPlaceholder() {
    return ":" + this.numParameters;
  }
}

export class MysqlDataApiQueryCompiler extends MysqlQueryCompiler {
  protected override appendValue(value: any) {
    const name = this.numParameters;
    this.append(this.getCurrentParameterPlaceholder());
    this.addParameter({
      name: name.toString(),
      value: serialize(value),
    });
  }

  protected override getCurrentParameterPlaceholder() {
    return ":" + this.numParameters;
  }
}

function serialize(value: any): Field {
  if (value == null) return { isNull: true };
  switch (typeof value) {
    case "number":
      if (Number.isInteger(value))
        return {
          longValue: value,
        };
      else
        return {
          doubleValue: value,
        };
    case "bigint":
      return {
        doubleValue: Number(value),
      };
    case "string":
      return {
        stringValue: value,
      };
    case "boolean":
      return {
        booleanValue: value,
      };
    case "object":
      if (Buffer.isBuffer(value))
        return {
          blobValue: value,
        };
      else break;
  }

  throw "wtf";
}
