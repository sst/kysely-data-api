import { SqlParameter } from "aws-sdk/clients/rdsdataservice";
import { MysqlQueryCompiler, PostgresQueryCompiler } from "kysely";

export class PostgresDataApiQueryCompiler extends PostgresQueryCompiler {
  protected override appendValue(value: unknown) {
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
  protected override appendValue(value: unknown) {
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

function serialize(value: unknown): Pick<SqlParameter, "typeHint" | "value"> {
  switch (typeof value) {
  case "bigint":
    return { value: { doubleValue: Number(value) } };
  case "boolean":
    return { value: { booleanValue: value } };
  case "number":
    if (Number.isInteger(value))
      return { value: { longValue: value } };
    else
      return { value: { doubleValue: value } };
  case "object":
    if (value == null)
      return { value: { isNull: true }};
    else if (Buffer.isBuffer(value))
      return { value: { blobValue: value } };
    else if (value instanceof Date)
      return {
        typeHint: "TIMESTAMP",
        value: { stringValue: fixISOString(value.toISOString()) },
      };
    else if ((value as RSU)?.value && isValueObject((value as RSU).value as RSU)) {
      if (
        (value as RSU).typeHint && ((value as RSU).value as RSU).stringValue
        && typeof ((value as RSU).value as RSU).stringValue === "string"
      )
        ((value as RSU).value as RSU).stringValue = fixStringValue(
          (value as RSS).typeHint,
          ((value as RSU).value as RSS).stringValue,
        );
      return value;
    }
    else
      break;
  case "string":
    return {
      value: { stringValue: value },
    };
  }

  throw new QueryCompilerError("Could not serialize value");
}

function fixStringValue(typeHint: SqlParameter["typeHint"], value: string) {
  switch (typeHint) {
  case "DATE":
    return parseToISOString(value).slice(0, 10);
  case "TIME":
    if (value.match(/^\d{4}-\d{2}-\d{2}/)) {
      return parseToISOString(value).slice(11, 23);
    }
    return fixTimeString(value);
  case "TIMESTAMP":
    return fixISOString(parseToISOString(value));
  }
  return value;
}

function fixTimeString(s: string) {
  const elements = (s || "00:00:00").split(":");
  while (elements.length < 3) {
    elements.push("00");
  }
  return elements.join(":").slice(0, 12);
}

function fixISOString(s: string) {
  return s.replace("T", " ").slice(0, 23);
}

function parseToISOString(s: string) {
  return new Date(Date.parse(s)).toISOString();
}

function isValueObject(value: Record<string, unknown>) {
  for (const key of primitiveKeys) {
    if (value[key]) {
      return true;
    }
  }
  if (value.arrayValue) {
    for (const key of arrayKeys) {
      if ((value.arrayValue as Record<string, unknown>)?.[key]) {
        return true;
      }
    }
  }
  return false;
}

class QueryCompilerError extends Error {
  constructor(message: string) {
    super(message);
    this.name = QueryCompilerError.name;
  }
}

const arrayKeys = ["booleanValues", "doubleValues", "longValues", "stringValues"];
const primitiveKeys = ["blobValue", "booleanValue", "doubleValue", "isNull", "longValue", "stringValue"];

type RSS = Record<string, string>;
type RSU = Record<string, unknown>;
