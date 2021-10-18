import { reset, db } from "./harness";

jest.setTimeout(1000 * 60);

beforeEach(async () => {
  await reset();
});

const person = {
  gender: "male",
  first_name: "jeff",
  last_name: "bezos",
} as const;

it("insert and read", async () => {
  await db
    .insertInto("person")
    .values({
      id: db.generated,
      ...person,
    })
    .execute();

  const result = await db.selectFrom("person").selectAll().execute();
  expect(result).toHaveLength(1);
  expect(result[0]).toMatchObject(person);
});

it("transaction", async () => {
  await db.transaction(async (tx) => {
    await tx
      .insertInto("person")
      .values({
        id: db.generated,
        ...person,
      })
      .execute();
  });

  const result = await db.selectFrom("person").selectAll().execute();
  expect(result).toHaveLength(1);
  expect(result[0]).toMatchObject(person);
});
