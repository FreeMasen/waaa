#[macro_use]
extern crate serde_derive;
use rusqlite::types::ToSql;
use rusqlite::{Connection, NO_PARAMS};
use std::fmt::Debug;
use std::io::Write;
use crossterm_cursor::{cursor, TerminalCursor};
type Res<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct FinalProduct {
    id: i64,
    name: String,
    manufacturer: String,
    macros: Macro,
    serving: ServingSize
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Macro {
    calories: f64,
    carbs: f64,
    fat: f64,
    protein: f64,
}
#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ServingSize {
    raw: SingleServing,
    household: SingleServing, 
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct SingleServing {
    value: f64,
    units: String,
}

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
        value REAL,
        unit TEXT,
        derivation_code TEXT NOT NULL)"
    }
    fn sql(&self, stmt: &mut rusqlite::Statement) -> Res<()> {
        stmt.execute(
        &[
            &self.nutrient_id as &dyn rusqlite::ToSql,
            &self.name,
            &self.id,
            &self.value,
            &self.unit,
            &self.derivation_code
        ]
        )?;
        Ok(())
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
            &self.id as &dyn rusqlite::ToSql,
            &self.name,
            &self.source,
            &self.upc,
            &self.manufacturer,
            &self.modified,
            &self.available,
            &self.ingredients,
        ])?;
        Ok(())
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
            &self.id as &dyn rusqlite::ToSql,
            &self.value,
            &self.unit,
            &self.household_value,
            &self.household_unit,
        ])?;
        Ok(())
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
    let conn = rusqlite::Connection::open_in_memory()?;//"nutr.sqlite")?;
    
    println!("capturing Nutrients");
    conn.execute(&Nutrient::create_sql(), NO_PARAMS)?;
    {
        let mut stmt = conn.prepare("INSERT INTO nutrients 
                                    VALUES (?1, ?2, ?3, ?4, ?5, ?6)")?;
        round_trip::<Nutrient>("nutr/Nutrients.csv", "nutr/nutrients.json", &mut stmt)?;
        println!("capturing products");
        conn.execute(&Product::create_sql(),  NO_PARAMS)?;
        let mut stmt = conn.prepare("INSERT INTO products
                                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)")?;
        round_trip::<Product>("nutr/Products.csv", "nutr/products.json", &mut stmt)?;
        println!("capturing servings");
        conn.execute(&Serving::create_sql(), NO_PARAMS)?;
        let mut stmt = conn.prepare("insert into serving 
                                    VALUES (?1, ?2, ?3, ?4, ?5)")?;
        round_trip::<Serving>("nutr/Serving_size.csv", "nutr/serving.json", &mut stmt)?;
        conn.execute(&Derivation::create_sql(), NO_PARAMS)?;
        let mut stmt = conn.prepare("INSERT INTO derivations
                                    VALUES (?1, ?2)")?;
        println!("capturing derivations");
        round_trip::<Derivation>("nutr/Derivation_Code_Description.csv", "nutr/derivation.json", &mut stmt)?;
    }
    println!("creating combined table");
    conn.execute(
"CREATE TABLE temp_nut (food_id INTEGER, energy REAL, carbs REAL, fat REAL, protein REAL);",
    NO_PARAMS)?;
    conn.execute("CREATE TABLE food_ids (id INTEGER NOT NULL UNIQUE)", NO_PARAMS)?;
    let id_ct = conn.execute("INSERT INTO food_ids
    SELECT id FROM products", NO_PARAMS)?;
    println!("creating energy table");
    conn.execute("CREATE TABLE energy (food_id INTEGER, value REAL DEFAULT 0)", NO_PARAMS)?;
    let en_ct = conn.execute("INSERT INTO energy
                SELECT products.id, nutrients.value
                FROM products
                LEFT JOIN nutrients
                ON products.id = nutrients.food_id
                and nutrients.id = ?1", &[&208])?;
    println!("energy count: {}\ncreating carbs table", en_ct);
    conn.execute("CREATE TABLE carbs (food_id INTEGER, value REAL DEFAULT 0)", NO_PARAMS)?;
    let c_ct = conn.execute("INSERT INTO carbs
                SELECT products.id, nutrients.value
                FROM products
                LEFT JOIN nutrients
                ON products.id = nutrients.food_id
                and nutrients.id = ?1", &[&205])?;
    println!("carbs count:{}\ncreating fat table", c_ct);
    conn.execute("CREATE TABLE fat (food_id INTEGER, value REAL DEFAULT 0)", NO_PARAMS)?;
    let f_ct = conn.execute("INSERT INTO fat
                SELECT products.id, nutrients.value
                FROM products
                LEFT JOIN nutrients
                ON products.id = nutrients.food_id
                and nutrients.id = ?1", &[&204])?;
    println!("fat count: {}\ncreating protein table", f_ct);
    conn.execute("CREATE TABLE protein (food_id INTEGER, value REAL DEFAULT 0)", NO_PARAMS)?;
    let p_ct = conn.execute("INSERT INTO protein
                SELECT products.id, nutrients.value
                FROM products
                LEFT JOIN nutrients
                ON products.id = nutrients.food_id
                and nutrients.id = ?1", &[&203])?;
    println!("protein count: {}\ninserting values into temp_nut", p_ct);
    
    println!("food id count: {}", id_ct);
    
    let full_ct = conn.execute("INSERT INTO temp_nut
                SELECT food_ids.id, energy.value, carbs.value, fat.value, protein.value
                from food_ids
                join energy
                    on food_ids.id = energy.food_id
                join carbs
                    on food_ids.id = carbs.food_id
                join fat
                    on food_ids.id = fat.food_id
                join protein
                    on food_ids.id = protein.food_id",
    NO_PARAMS)?;
    conn.execute("CREATE TABLE final_products (id INTEGER, name TEXT, manufacturer TEXT,
                value REAL, unit STRING, household_value REAL, household_unit TEXT,
                calories REAL, carbs REAL, fat REAL, protein REAL);", NO_PARAMS)?;
    conn.execute("INSERT INTO final_products
                SELECT products.id, products.name, products.manufacturer,
                serving.value, serving.unit, serving.household_value, serving.household_unit,
                temp_nut.energy, temp_nut.carbs, temp_nut.fat, temp_nut.protein
        FROM products
            JOIN serving
                ON products.id = serving.food_id
            JOIN temp_nut
                ON products.id = temp_nut.food_id;", NO_PARAMS)?;
    let mut fin = conn.prepare(
        "SELECT products.id, products.name, products.manufacturer,
                serving.value, serving.unit, serving.household_value, serving.household_unit,
                temp_nut.energy, temp_nut.carbs, temp_nut.fat, temp_nut.protein
        FROM products
            JOIN serving
                ON products.id = serving.food_id
                and (serving.value is not null 
                    or serving.household_value is not null)
            JOIN temp_nut
                ON products.id = temp_nut.food_id
                and temp_nut.energy is not null
                and temp_nut.carbs is not null
                and temp_nut.fat is not null
                and temp_nut.protein is not null
                and temp_nut.energy > 0
                and temp_nut.carbs > 0
                and temp_nut.fat > 0
                and temp_nut.protein > 0
        "
    )?;
    println!("full count: {}\n", full_ct);
    assert_eq!(full_ct, id_ct);
    let result: Vec<FinalProduct> = fin.query_map(NO_PARAMS, |row|{ 
    let id: i64 = row.get(0).expect("failed to get 1 as id");
    let name: String = row.get(1).expect("failed to get 2 as name");
    let manufacturer: String = row.get(2).expect("failed to get 3 as manufacturer");
    let value: Option<f64> = row.get(3).expect("failed to get 4 as value");
    let units: String = row.get(4).expect("failed to get 5 as units");
    let household_value: Option<f64> = row.get(5).expect("failed to get 6 as household_value");
    let household_units: String = row.get(6).expect("failed to get 7 as household_units");
    let calories: f64 = row.get(7)?;
    let carbs: f64 = row.get(8)?;
    let fat: f64 = row.get(9)?;
    let protein: f64 = row.get(10)?;
    Ok(
        FinalProduct {
            id,
            name,
            manufacturer,
            serving: ServingSize {
                raw: SingleServing {
                    units,
                    value: value.unwrap_or(0f64),
                },
                household: SingleServing {
                    value: household_value.unwrap_or(0f64),
                    units: household_units,
                },
            },
            macros: Macro {
                calories: calories,
                carbs: carbs,
                fat: fat,
                protein: protein,
            }
        }
    )})?.filter_map(|p| p.ok()).collect();
    ::std::fs::write("test.json", serde_json::to_string_pretty(&result)?)?;
    conn.execute_batch("DROP TABLE energy;DROP TABLE carbs;DROP TABLE fat;DROP TABLE protein;DROP TABLE food_ids;DROP TABLE temp_nut;")?;
    println!("Starting backup\n");
    let mut on_disk = rusqlite::Connection::open("nutr.sqlite")?;
    rusqlite::backup::Backup::new(&conn, &mut on_disk)?.run_to_completion(
        250, ::std::time::Duration::from_secs(1), 
        Some(|p| report(p.pagecount as usize, p.pagecount as usize-p.remaining as usize))
    )?;

    Ok(())
}
fn report(total: usize, current: usize) {
    let mut c = cursor();
    let (_, y) = c.pos();
    // eprintln!("{}", y);
    c.move_left(y);
    c.move_up(1);
    let mut b = progress_string::BarBuilder::new()
                .total(total)
                .include_percent()
                .include_numbers()
                .get_bar();
    b.replace(current);
    ::std::io::stdout().flush().unwrap();
    println!("{}", b.to_string());
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

fn from_row(row: rusqlite::Row) -> Res<FinalProduct> {
    let id: i64 = row.get(1)?;
    let name: String = row.get(2)?;
    let manufacturer: String = row.get(3)?;
    let value: f64 = row.get(4)?;
    let units: String = row.get(5)?;
    let household_value: f64 = row.get(6)?;
    let household_units: String = row.get(7)?;
    let calories: f64 = row.get(8)?;
    let carbs: f64 = row.get(9)?;
    let fat: f64 = row.get(10)?;
    let protein: f64 = row.get(11)?;
    Ok(
        FinalProduct {
            id,
            name,
            manufacturer,
            serving: ServingSize {
                raw: SingleServing {
                    units,
                    value,
                },
                household: SingleServing {
                    value: household_value,
                    units: household_units,
                },
            },
            macros: Macro {
                calories,
                carbs,
                fat,
                protein
            }
        }
    )
}