// src/lib.rs

use std::{fmt::write, io::{self, Write}, ptr, str::from_utf8};

pub const COLUMN_ID_SIZE: usize = 4;
pub const COLUMN_USERNAME_SIZE: usize = 32;
pub const COLUMN_EMAIL_SIZE: usize = 255;
pub const COLUMN_ID_OFFSET: usize = 0;
pub const COLUMN_USERNAME_OFFSET: usize = COLUMN_ID_SIZE;
pub const COLUMN_EMAIL_OFFSET: usize = COLUMN_USERNAME_OFFSET + COLUMN_USERNAME_SIZE;
pub const ROW_SIZE: usize = COLUMN_ID_SIZE + COLUMN_USERNAME_SIZE + COLUMN_EMAIL_SIZE;
pub const PAGE_SIZE: usize = 4096;
pub const MAX_PAGES: usize = 100;
pub const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
pub const MAX_ROWS: usize = MAX_PAGES * ROWS_PER_PAGE;

#[derive(Debug, PartialEq)]
#[repr(C)]
pub struct Row {
    pub id: u32,
    pub username: [u8; COLUMN_USERNAME_SIZE],
    pub email: [u8; COLUMN_EMAIL_SIZE],
}

impl Row {
    pub fn new(id: u32, username: String, email: String) -> Row {
        let mut row = Row {
            id,
            username: [0; COLUMN_USERNAME_SIZE],
            email: [0; COLUMN_EMAIL_SIZE],
        };
        let username_bytes = username.as_bytes();
        let email_bytes = email.as_bytes();
        row.username[..username_bytes.len()].copy_from_slice(username_bytes);
        row.email[..email_bytes.len()].copy_from_slice(email_bytes);
        row
    }

    pub fn write<W: Write>(&self, writer: &mut W) {
        let username = from_utf8(&self.username).unwrap_or("Invalid UTF-8");
        let email = from_utf8(&self.email).unwrap_or("Invalid UTF-8");
        writeln!(
            writer,
            "{} {} {}",
            self.id,
            username.trim_end_matches('\0'),
            email.trim_end_matches('\0')
        ).unwrap();
    }
    pub fn to_string(&self) -> String{
        let username = from_utf8(&self.username).unwrap_or("Invalid UTF-8");
        let email = from_utf8(&self.email).unwrap_or("Invalid UTF-8");
        format!(
            "{} {} {}",
            self.id,
            username.trim_end_matches('\0'),
            email.trim_end_matches('\0')
        )
    }
}

#[derive(Debug)]
pub struct Table {
    pub num_rows: usize,
    pages: [Option<Box<[u8; PAGE_SIZE]>>; MAX_PAGES],
}

impl Table {
    pub fn new() -> Self {
        Self {
            num_rows: 0,
            pages: [(); MAX_PAGES].map(|_| None),
        }
    }

    pub fn row_slot(&mut self, index: usize) -> &mut [u8] {
        let page_num = index / ROWS_PER_PAGE;
        if page_num >= MAX_PAGES {
            panic!("Page number out of bounds");
        }
        if self.pages[page_num].is_none() {
            self.pages[page_num] = Some(Box::new([0; PAGE_SIZE]));
        }
        let page = self.pages[page_num].as_mut().unwrap();
        let row_offset = index % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;
        &mut page[byte_offset..byte_offset + ROW_SIZE]
    }
}

pub fn serialize(row: &Row, dest: &mut [u8]) {
    unsafe {
        ptr::copy_nonoverlapping(
            &row.id as *const u32 as *const u8,
            dest.as_mut_ptr().add(COLUMN_ID_OFFSET),
            COLUMN_ID_SIZE,
        );
        ptr::copy_nonoverlapping(
            row.username.as_ptr(),
            dest.as_mut_ptr().add(COLUMN_USERNAME_OFFSET),
            COLUMN_USERNAME_SIZE,
        );
        ptr::copy_nonoverlapping(
            row.email.as_ptr(),
            dest.as_mut_ptr().add(COLUMN_EMAIL_OFFSET),
            COLUMN_EMAIL_SIZE,
        );
    }
}

pub fn deserialize(src: &[u8]) -> Row {
    let mut row = Row {
        id: 0,
        username: [0; COLUMN_USERNAME_SIZE],
        email: [0; COLUMN_EMAIL_SIZE],
    };
    unsafe {
        ptr::copy_nonoverlapping(
            src.as_ptr().add(COLUMN_ID_OFFSET),
            &mut row.id as *mut u32 as *mut u8,
            COLUMN_ID_SIZE,
        );
        ptr::copy_nonoverlapping(
            src.as_ptr().add(COLUMN_USERNAME_OFFSET),
            row.username.as_mut_ptr(),
            COLUMN_USERNAME_SIZE,
        );
        ptr::copy_nonoverlapping(
            src.as_ptr().add(COLUMN_EMAIL_OFFSET),
            row.email.as_mut_ptr(),
            COLUMN_EMAIL_SIZE,
        );
    }
    row
}

macro_rules! scan {
    ( $string:expr, $sep:expr, $( $x:ty ),+ ) => {{
        let mut iter = $string.split($sep);
        ($(iter.next().and_then(|word| word.parse::<$x>().ok()),)*)
    }}
}

#[derive(Debug)]
pub enum MetaCommandResult {
    Success,
    Exit,
    Unrecognized,
}

#[derive(Debug)]
pub enum PrepareResult {
    Success,
    SyntaxError,
    Unrecognized,
}

#[derive(Debug)]
pub enum ExecuteResult {
    Success,
    TableFull,
}

#[derive(Debug)]
pub enum StatementType {
    Insert,
    Select,
}

#[derive(Debug)]
pub struct Statement {
    statement_type: StatementType,
    row_to_insert: Option<Row>,
}

pub fn print_prompt<W: Write>(writer: &mut W) {
    write!(writer, "rsql > ").unwrap();
    writer.flush().expect("flush failed!");
}

pub fn do_meta_command<W: Write>(buf: &str, writer: &mut W) -> MetaCommandResult {
    if buf == ".exit" {
        return MetaCommandResult::Exit;
    }
    writeln!(writer, "Unrecognized Command '{}'.", buf).unwrap();
    MetaCommandResult::Unrecognized
}

pub fn prepare_statement(buf: &str, statement: &mut Statement) -> PrepareResult {
    if buf.starts_with("insert") {
        statement.statement_type = StatementType::Insert;
        let input = scan!(buf, char::is_whitespace, String, u32, String, String);
        if let (Some(_), Some(id), Some(username), Some(email)) = input {
            statement.row_to_insert = Some(Row::new(id, username, email));
            return PrepareResult::Success;
        }
        return PrepareResult::SyntaxError;
    }
    if buf.starts_with("select") {
        statement.statement_type = StatementType::Select;
        return PrepareResult::Success;
    }
    PrepareResult::Unrecognized
}

pub fn execute_statement<W: Write>(table: &mut Table, statement: &Statement, writer: &mut W) -> ExecuteResult {
    match statement.statement_type {
        StatementType::Insert => {
            if let Some(row) = &statement.row_to_insert {
                execute_insert(table, row)
            } else {
                ExecuteResult::Success
            }
        }
        StatementType::Select => execute_select(table, writer),
    }
}

pub fn execute_insert(table: &mut Table, row: &Row) -> ExecuteResult {
    if table.num_rows >= MAX_ROWS {
        return ExecuteResult::TableFull;
    }
    serialize(row, table.row_slot(table.num_rows));
    table.num_rows += 1;
    ExecuteResult::Success
}

pub fn execute_select<W: Write>(table: &mut Table, writer: &mut W) -> ExecuteResult {
    for i in 0..table.num_rows {
        let row = deserialize(table.row_slot(i));
        row.write(writer);
    }
    ExecuteResult::Success
}

pub fn run_repl<R: io::BufRead, W: Write>(table: &mut Table, reader: &mut R, writer: &mut W) {
    let mut input_buffer = String::new();

    loop {
        print_prompt(writer);
        input_buffer.clear();

        if reader.read_line(&mut input_buffer).is_err() {
            writeln!(writer, "Error reading input").unwrap();
            break;
        }

        let input = input_buffer.trim();

        if input.starts_with('.') {
            match do_meta_command(input, writer) {
                MetaCommandResult::Exit => break,
                MetaCommandResult::Unrecognized => continue,
                MetaCommandResult::Success => continue,
            }
        }

        let mut statement = Statement {
            statement_type: StatementType::Insert,
            row_to_insert: None,
        };

        match prepare_statement(input, &mut statement) {
            PrepareResult::Success => {
                match execute_statement(table, &statement, writer) {
                    ExecuteResult::Success => {},
                    ExecuteResult::TableFull => writeln!(writer, "Row not inserted, table full '{}'", statement.row_to_insert.expect("Row not initialized panic").to_string()).unwrap()

                }
            }
            PrepareResult::SyntaxError => writeln!(writer, "Syntax Error in '{}'", input).unwrap(),
            PrepareResult::Unrecognized => writeln!(writer, "Unrecognized keyword at start of '{}'", input).unwrap(),
        }
    }
}

// unit tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_creation() {
        let row = Row::new(1, "testuser".to_string(), "test@example.com".to_string());
        assert_eq!(row.id, 1);
        assert_eq!(&row.username[..8], b"testuser");
        assert_eq!(&row.email[..16], b"test@example.com");
    }

    #[test]
    fn test_serialize_deserialize() {
        let original_row = Row::new(1, "testuser".to_string(), "test@example.com".to_string());
        let mut buffer = vec![0u8; ROW_SIZE];

        serialize(&original_row, &mut buffer);
        let deserialized_row = deserialize(&buffer);

        assert_eq!(deserialized_row, original_row);
    }

    #[test]
    fn test_row_layout() {
        assert_eq!(std::mem::size_of::<Row>(), 1 + 4 + 32 + 255); // 291 bytes
        assert_eq!(std::mem::align_of::<Row>(), 4); // u32 alignment
    }
}
