#[macro_use]
extern crate serde_derive;
use rusqlite::types::ToSql;
use rusqlite::{Connection, NO_PARAMS};
use std::fmt::Debug;


type Res<T> = Result<T, Box<dyn std::error::Error>>;


#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Nutrient {
    #[serde(alias = "NDB_No")]
    id: i64,
    #[serde(alias = "Nutrient_Code")]
    nutrient_id: i64,
    #[serde(alias = "Nutrient_name")]
    name: String,
    #[serde(alias = "Derivation_Code")]
    derivation_code: String,
    #[serde(alias = "Output_value")]
    value: f64,
    #[serde(alias = "Output_uom")]
    unit: String,
}
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Product {
    #[serde(alias = "long_name")]
    name: String,
    #[serde(alias = "NDB_Number")]
    id: i64,
    #[serde(alias = "data_source")]
    source: String,
    #[serde(alias = "gtin_upc")]
    upc: String,
    #[serde(alias = "manufacturer")]
    manufacturer: String,
    #[serde(alias = "date_modified")]
    modified: String,
    #[serde(alias = "date_available")]
    available: String,
    #[serde(alias = "ingredients_english")]
    ingredients: String,
}
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Serving {
    #[serde(alias = "NDB_No")]
    id: i64,
    #[serde(alias = "Serving_Size")]
    value: Option<f64>,
    #[serde(alias = "Serving_Size_UOM")]
    unit: String,
    #[serde(alias = "Household_Serving_Size")]
    household_value: Option<f64>,
    #[serde(alias = "Household_Serving_Size_UOM")]
    household_unit: String,
    #[serde(alias = "Preparation_State")]
    prep_state: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Derivation {
    code: String,
    desc: String,
}

trait GetId {
    fn id(&self) -> i64;
    fn name(&self) -> &str;
    fn sql(&self, stmt: &mut rusqlite::Statement) -> Res<()>;
    fn create_sql() -> &'static str;
}
impl GetId for Nutrient {
    fn id(&self) -> i64 {
        self.nutrient_id
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn create_sql() -> &'static str {
        "CREATE TABLE nutrients
        (id INT NOT NULL,
        name TEXT NOT NULL,
        food_id INT NOT NULL,
        derivation_code TEXT NOT NULL)"
    }
    fn sql(&self, stmt: &mut rusqlite::Statement) -> Res<()> {
        stmt.execute(
        &[
            &self.nutrient_id as &rusqlite::ToSql,
            &self.name,
            &self.id,
            &self.derivation_code
        ]
        )?;
        Ok(())
        // format!("INSERT INTO nutrients
        //         VALUES ({}, '{}', {}, '{}')",
        //     self.nutrient_id,
        //     self.name,
        //     self.id,
        //     self.derivation_code)
    }
}
impl GetId for Derivation {
    fn id(&self) -> i64 {
        0
    }
    fn name(&self) -> &str {
        &self.code
    }
    fn sql(&self, stmt: &mut rusqlite::Statement) -> Res<()> {
        stmt.execute(&[
            &self.code,
            &self.desc
        ])?;
        Ok(())
        // format!("INSERT INTO derivations
        //         VALUES ('{}', '{}'",
        //     self.code, self.desc)
    }
    fn create_sql() -> &'static str {
        "CREATE TABLE derivations 
        (code TEXT, desc TEXT)"
    }
}
impl GetId for Product {
    fn id(&self) -> i64 {
        self.id
    }
    fn name(&self) -> &str {
        &self.name
    }
    fn sql(&self, stmt: &mut rusqlite::Statement) -> Res<()> {
        stmt.execute(&[
            &self.id as &rusqlite::ToSql,
            &self.name,
            &self.source,
            &self.upc,
            &self.manufacturer,
            &self.modified,
            &self.available,
            &self.ingredients,
        ])?;
        Ok(())
        // format!("INSERT INTO products
        //         VALUES ({}, '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
        //     self.id,
        //     self.name,
        //     self.source,
        //     self.upc,
        //     self.manufacturer,
        //     self.modified,
        //     self.available,
        //     self.ingredients
        // )
    }
    fn create_sql() -> &'static str {
        "CREATE TABLE products (
            id INTEGER NOT NULL,
            name TEXT NOT NULL,
            source TEXT,
            upc TEXT,
            manufacturer TEXT,
            modified TEXT,
            available TEXT,
            ingredients TEXT
        )"
    }
}
impl GetId for Serving {
    fn id(&self) -> i64 {
        self.id
    }
    fn name(&self) -> &str {
        ""
    }
    fn sql(&self, stmt: &mut rusqlite::Statement) -> Res<()> {
        stmt.execute(
        &[
            &self.id as &rusqlite::ToSql,
            &self.value,
            &self.unit,
            &self.household_value,
            &self.household_unit,
        ])?;
        Ok(())
        // format!("insert into serving 
        //     VALUES ({}, {}, '{}', {} '{}')", 
        //     self.id, 
        //     handle_option(&self.value), 
        //     self.unit, self.household_unit, 
        //     handle_option(&self.household_value)
        // )
    }
    fn create_sql() -> &'static str {
        "CREATE TABLE serving (
            food_id INTEGER NOT NULL, 
            value REAL, 
            unit TEXT, 
            household_value REAL, 
            household_unit TEXT
        )"
    }
}

fn main() -> Res<()> {
    let conn = rusqlite::Connection::open("nutr.sqlite")?;
    println!("capturing Nutrients");
    conn.execute(&Nutrient::create_sql(), NO_PARAMS)?;
    let mut stmt = conn.prepare("INSERT INTO nutrientsVALUES (?1, ?2, ?3, ?4)")?;
    round_trip::<Nutrient>("nutr/Nutrients.csv", "nutr/nutrients.json", &mut stmt)?;
    println!("capturing products");
    conn.execute(&Product::create_sql(),  NO_PARAMS)?;
    let mut stmt = conn.prepare("INSERT INTO products
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)")?;
    round_trip::<Product>("nutr/Products.csv", "nutr/products.json", &mut stmt)?;
    println!("capturing servings");
    conn.execute(&Serving::create_sql(), NO_PARAMS)?;
    let mut stmt = conn.prepare("insert into serving VALUES (?1, ?2. ?3. ?4, ?5)")?;
    round_trip::<Serving>("nutr/Serving_size.csv", "nutr/serving.json", &mut stmt)?;
    conn.execute(&Derivation::create_sql(), NO_PARAMS)?;
    let mut stmt = conn.prepare("INSERT INTO derivations
                 VALUES (?1, ?2)")?;
    println!("capturing derivations");
    round_trip::<Derivation>("nutr/Derivation_Code_Description.csv", "nutr/derivation.json", &mut stmt)?;
    Ok(())
}

fn round_trip<T>(from: &str, to: &str, s: &mut rusqlite::Statement) -> Res<()> 
where T: serde::de::DeserializeOwned + serde::ser::Serialize + GetId + Debug {
    use std::io::{Write, BufWriter};
    use std::fs::File;
    // let f = File::create(to)?;
    // let mut b = BufWriter::new(f);
    // b.write(b"[")?;
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .quote(b'"')
        .quoting(true)
        .from_path(from)?;
    let mut after_first = false;
    let mut i = 0;
    for row in reader.deserialize::<T>() {
        let de = row?;
        if after_first {
            // b.write(b",")?;
        } else {
            after_first = true;
        }
        de.sql(s)?;
        // let s = serde_json::to_string(&de)?;
        // b.write(
        //     s.as_bytes()
        // )?;
        i += 1;
        if i % 1_000_000 == 0 {
            dbg!(i);
        }
    }
    // b.write(b"\n]")?;
    Ok(())
}