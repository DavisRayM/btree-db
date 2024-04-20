use super::cursor::Cursor;

/// Database commands/statements
#[derive(Debug, Clone)]
pub enum Statement {
    Select,
    Insert(u64, String),
}

impl Statement {
    pub fn execute(&self, cursor: &mut Cursor) {
        match self {
            Self::Select => {
                cursor.select().iter().for_each(|s| {
                    println!("{}", s);
                });
            }
            Self::Insert(id, content) => match cursor.insert(*id, content) {
                Err(e) => println!("error: {e}"),
                _ => (),
            },
        }
    }
}

impl TryInto<Statement> for &str {
    type Error = String;

    fn try_into(self) -> Result<Statement, Self::Error> {
        let value = self.trim();

        if value == "select" {
            Ok(Statement::Select)
        } else if value.starts_with("insert") {
            let data = value.split(' ').collect::<Vec<&str>>();
            if data.len() < 3 {
                return Err("invalid syntax".to_string());
            }

            let id = data[1].parse::<u64>().unwrap();
            let content = data
                .iter()
                .skip(2)
                .map(|s| String::from(*s))
                .collect::<Vec<String>>();
            let content = content.join(" ");

            Ok(Statement::Insert(id, content))
        } else {
            Err(format!("unknown command `{value}`."))
        }
    }
}
