import { SqlParameter } from "aws-sdk/clients/rdsdataservice";
import { MysqlQueryCompiler, PostgresQueryCompiler } from "kysely";

export class PostgresDataApiQueryCompiler extends PostgresQueryCompiler {
  protected override appendValue(value: any) {
    const name = this.numParameters;
    this.append(this.getCurrentParameterPlaceholder());
    this.addParameter({
      name: name.toString(),
      ...serialize(value),
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
      ...serialize(value),
    });
  }

  protected override getCurrentParameterPlaceholder() {
    return ":" + this.numParameters;
  }
}

function serialize(value: any): SqlParameter {
  if (value == null) return { value: { isNull: true } };
  switch (typeof value) {
    case "number":
      if (Number.isInteger(value))
        return {
          value: {
            longValue: value,
          },
        };
      else
        return {
          value: {
            doubleValue: value,
          }
        };
    case "bigint":
      return {
        value: {
          doubleValue: Number(value),
        }
      };
    case "string":
      return {
        value: {
          stringValue: value,
        }
      };
    case "boolean":
      return {
        value: {
          booleanValue: value,
        }
      };
    case "object":
      if (Buffer.isBuffer(value))
        return {
          value: {
            blobValue: value,
          }
        };

      if (Array.isArray(value)) {
        return {
          value: {
            arrayValue: {
              stringValues: value,
            }
          }
        }
      }

      if (value instanceof Date)
        return {
          typeHint: "TIMESTAMP",
          value: { stringValue: serializeDate(value) },
        }

      return {
        value: {
          stringValue: JSON.stringify(value),
        }
      }
  }

  throw new Error(`Unsupported type: ${value}`);
}

function serializeDate(input: Date) {
  return input.toISOString().replace("T", " ").substring(0, 23);
}
