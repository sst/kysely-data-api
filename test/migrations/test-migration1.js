async function up(db) {
  await db.schema
    .createTable("person")
    .addColumn("id", "integer", (col) => col.increments().primaryKey())
    .addColumn("first_name", "varchar")
    .addColumn("last_name", "varchar")
    .addColumn("gender", "varchar(50)")
    .execute();

  await db.schema
    .createTable("pet")
    .addColumn("id", "integer", (col) => col.increments().primaryKey())
    .addColumn("name", "varchar", (col) => col.notNull().unique())
    .addColumn("owner_id", "integer", (col) => col.references("person.id").onDelete("cascade"))
    .addColumn("species", "varchar")
    .execute();

  await db.schema
    .createIndex("pet_owner_id_index")
    .on("pet")
    .column("owner_id")
    .execute();
}

async function down(db) {
  await db.schema.dropTable("person").execute();
  await db.schema.dropTable("pet").execute();
}

module.exports = {
  up,
  down,
};
