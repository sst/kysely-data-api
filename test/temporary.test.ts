import { migrate, db, reset } from "./harness";

jest.setTimeout(1000 * 60);

beforeAll(async () => {
  await migrate();
});

beforeEach(async () => {
  await reset();
});

const PERSON = {
  gender: "male",
  first_name: "jeff",
  last_name: "bezos",
} as const;

const PERSON_ALIAS = {
    first: "jeff",
    last: "bezos"
}

it("insert and read", async () => {
  await db
    .insertInto("person")
    .values({
      id: db.generated,
      ...PERSON,
    })
    .execute();

  const result = await db.selectFrom("person").selectAll().execute();
  expect(result).toHaveLength(1);
  expect(result[0]).toMatchObject(PERSON);
});

it("alias return", async () => {
    const result = await db.selectFrom("person").select(["first_name as first", "last_name as last"]).execute();
    expect(result).toHaveLength(1);
    expect(result[0]).toMatchObject(PERSON_ALIAS);
});

it("join", async () => {
  const person = await db
    .insertInto("person")
    .values({
      id: db.generated,
      ...PERSON,
    })
    .returning(["id"])
    .executeTakeFirst();

  await db
    .insertInto("pet")
    .values({
      id: db.generated,
      name: "fido",
      species: "dog",
      owner_id: person.id,
    })
    .execute();

  const result = await db
    .selectFrom("person")
    .innerJoin("pet", "pet.owner_id", "person.id")
    .select(["pet.name as pet_name", "person.first_name"])
    .executeTakeFirst();

  expect(result.first_name).toEqual("jeff");
  expect(result.pet_name).toEqual("fido");
});

it("transaction", async () => {
  await db.transaction().execute(async (tx) => {
    await tx
      .insertInto("person")
      .values({
        id: db.generated,
        ...PERSON,
      })
      .execute();
  });

  const result = await db.selectFrom("person").selectAll().execute();
  expect(result).toHaveLength(1);
  expect(result[0]).toMatchObject(PERSON);
});
