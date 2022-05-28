import { sql } from "kysely";

import { db } from "./harness";

const { ref } = db.dynamic;

test("bigint", () => {
  const result = db.selectFrom("person").selectAll().where("score", ">", BigInt(1000)).compile();
  expect(result.sql).toEqual(`select * from "person" where "score" > :0`);
  expect(result.parameters).toEqual([{ name: "0", value: { doubleValue: 1000 } }]);
});

test("boolean", () => {
  const result = db.selectFrom("person").selectAll().where("is_active", "=", true).compile();
  expect(result.sql).toEqual(`select * from "person" where "is_active" = :0`);
  expect(result.parameters).toEqual([{ name: "0", value: { booleanValue: true } }]);
});

test("object (Array (bigint))", () => {
  const result = db
    .selectFrom("person")
    .selectAll().where("score", "in", [BigInt(10), BigInt(100), BigInt(1000)])
    .compile();
  expect(result.sql).toEqual(`select * from "person" where "score" in (:0, :1, :2)`);
  expect(result.parameters).toEqual([
    {
      name: "0",
      value: {
        doubleValue: 10,
      },
    },
    {
      name: "1",
      value: {
        doubleValue: 100,
      },
    },
    {
      name: "2",
      value: {
        doubleValue: 1000,
      },
    },
  ]);
});

test("object (Array (boolean))", () => {
  const result = db.selectFrom("person").selectAll().where("is_active", "in", [true, false]).compile();
  expect(result.sql).toEqual(`select * from "person" where "is_active" in (:0, :1)`);
  expect(result.parameters).toEqual([
    {
      name: "0",
      value: {
        booleanValue: true,
      },
    },
    {
      name: "1",
      value: {
        booleanValue: false,
      },
    },
  ]);
});

test("object (Array (number))", () => {
  const result = db.selectFrom("person").selectAll().where("id", "in", [1, 2, 3]).compile();
  expect(result.sql).toEqual(`select * from "person" where "id" in (:0, :1, :2)`);
  expect(result.parameters).toEqual([
    {
      name: "0",
      value: {
        longValue: 1,
      },
    },
    {
      name: "1",
      value: {
        longValue: 2,
      },
    },
    {
      name: "2",
      value: {
        longValue: 3,
      },
    },
  ]);
});

test("object (Array (string))", () => {
  const result = db
    .selectFrom("person")
    .selectAll().where(sql`lower(first_name)`, "in", ["alice", "bob", "carol"])
    .compile();
  expect(result.sql).toEqual(`select * from "person" where lower(first_name) in (:0, :1, :2)`);
  expect(result.parameters).toEqual([
    {
      name: "0",
      value: {
        stringValue: "alice",
      },
    },
    {
      name: "1",
      value: {
        stringValue: "bob",
      },
    },
    {
      name: "2",
      value: {
        stringValue: "carol",
      },
    },
  ]);
});

test("object (Buffer)", () => {
  const result = db
    .selectFrom("person")
    .selectAll()
    .where("avatar", "=", Buffer.from("abc"))
    .compile();
  expect(result.sql).toEqual(`select * from "person" where "avatar" = :0`);
  expect(result.parameters).toEqual([{ name: "0", value: { blobValue: Buffer.from("abc") } }]);
});

test("object (Date)", () => {
  const result = db
    .selectFrom("person")
    .selectAll()
    .where("created_at", "<", new Date(Date.UTC(2022, 9, 20, 12, 34, 56, 789)))
    .compile();
  expect(result.sql).toEqual(`select * from "person" where "created_at" < :0`);
  expect(result.parameters).toEqual([
    { name: "0", typeHint: "TIMESTAMP", value: { stringValue: "2022-10-20 12:34:56.789" }},
  ]);
});

test("object (null)", () => {
  const result = db.selectFrom("pet").selectAll().where("owner_id", "=", null).compile();
  expect(result.sql).toEqual(`select * from "pet" where "owner_id" = :0`);
  expect(result.parameters).toEqual([{ name: "0", value: { isNull: true } }]);
});

test("object (value object (primitives))", () => {
  const inputs = {
    blobValue: Buffer.from("abc"),
    booleanValue: true,
    doubleValue: 1.23,
    isNull: true,
    longValue: 123,
    stringValue: "abc",
  };
  for (const [key, value] of Object.entries(inputs)) {
    const result = db
      .selectFrom("person")
      .selectAll()
      .where(ref("foo"), "=", { value: { [key]: value } })
      .compile();
    expect(result.sql).toEqual(`select * from "person" where "foo" = :0`);
    expect(result.parameters).toEqual([{ name: "0", value: { [key]: value } }]);
  }
});

test("object (value object (arrays))", () => {
  const inputs = {
    booleanValues: [true, false],
    doubleValues: [1.23, 4.56, 7.89],
    longValues: [123, 456, 789],
    stringValues: ["abc", "def", "ghi"],
  };
  for (const [key, value] of Object.entries(inputs)) {
    const result = db
      .selectFrom("person")
      .selectAll()
      .where(ref("foo"), "in", { value: { arrayValue: { [key]: value } } })
      .compile();
    expect(result.sql).toEqual(`select * from "person" where "foo" in :0`);
    expect(result.parameters).toEqual([{ name: "0", value: { arrayValue: { [key]: value } } }]);
  }
});

test("object (value object (DATE))", () => {
  const pairs = [
    ["2022-10-20", "2022-10-20"],
    ["2022-10-20T01:34:56+02:00", "2022-10-19"],
    ["2022-10-20T12:34:56.789123Z", "2022-10-20"],
  ];
  for (const [stringValue, fixedStringValue] of pairs) {
    const result = db.selectFrom("person").selectAll().where("created_at", "<", {
      typeHint: "DATE",
      value: { stringValue },
    } as any).compile(); // eslint-disable-line @typescript-eslint/no-explicit-any
    expect(result.sql).toEqual(`select * from "person" where "created_at" < :0`);
    expect(result.parameters).toEqual([{ name: "0", typeHint: "DATE", value: { stringValue: fixedStringValue } }]);
  }
});

test("object (value object (DECIMAL))", () => {
  const result = db.selectFrom("person").selectAll().where("balance", ">", {
    typeHint: "DECIMAL",
    value: { stringValue: "1000" },
  } as any).compile(); // eslint-disable-line @typescript-eslint/no-explicit-any
  expect(result.sql).toEqual(`select * from "person" where "balance" > :0`);
  expect(result.parameters).toEqual([{ name: "0", typeHint: "DECIMAL", value: { stringValue: "1000" } }]);
});

test("object (value object (TIME))", () => {
  const pairs = [
    ["12:34", "12:34:00"],
    ["12:34:56", "12:34:56"],
    ["12:34:56.789123", "12:34:56.789"],
    ["2022-10-20", "00:00:00.000"],
    ["2022-10-20T01:34:56+02:00", "23:34:56.000"],
    ["2022-10-20T12:34:56.789123Z", "12:34:56.789"],
  ];
  for (const [stringValue, fixedStringValue] of pairs) {
    const result = db.selectFrom("person").selectAll().where("created_at", "<", {
      typeHint: "TIME",
      value: { stringValue },
    } as any).compile(); // eslint-disable-line @typescript-eslint/no-explicit-any
    expect(result.sql).toEqual(`select * from "person" where "created_at" < :0`);
    expect(result.parameters).toEqual([{ name: "0", typeHint: "TIME", value: { stringValue: fixedStringValue } }]);
  }
});

test("object (value object (TIMESTAMP))", () => {
  const pairs = [
    ["2022-10-20", "2022-10-20 00:00:00.000"],
    ["2022-10-20T01:34:56+02:00", "2022-10-19 23:34:56.000"],
    ["2022-10-20T12:34:56.789123Z", "2022-10-20 12:34:56.789"],
  ];
  for (const [stringValue, fixedStringValue] of pairs) {
    const result = db.selectFrom("person").selectAll().where("created_at", "<", {
      typeHint: "TIMESTAMP",
      value: { stringValue },
    } as any).compile(); // eslint-disable-line @typescript-eslint/no-explicit-any
    expect(result.sql).toEqual(`select * from "person" where "created_at" < :0`);
    expect(result.parameters).toEqual([
      { name: "0", typeHint: "TIMESTAMP", value: { stringValue: fixedStringValue } },
    ]);
  }
});

test("number", () => {
  const result = db.selectFrom("person").selectAll().where("id", "=", 1).compile();
  expect(result.sql).toEqual(`select * from "person" where "id" = :0`);
  expect(result.parameters).toEqual([{ name: "0", value: { longValue: 1 } }]);
});

test("string", () => {
  const result = db.selectFrom("person").selectAll().where("first_name", "ilike", "%john%").compile();
  expect(result.sql).toEqual(`select * from "person" where "first_name" ilike :0`);
  expect(result.parameters).toEqual([{ name: "0", value: { stringValue: "%john%" } }]);
});

test("throwing with unknown value type", () => {
  expect(() => db
    .selectFrom("person")
    .selectAll()
    .where("id", "=", {} as any) // eslint-disable-line @typescript-eslint/no-explicit-any
    .compile(),
  ).toThrow("Could not serialize value");
});
