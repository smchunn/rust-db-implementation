use core::panic;
use std::{io, mem::MaybeUninit, process::exit, ptr, str::from_utf8, usize};
macro_rules! scan {
    ( $string:expr, $sep:expr, $( $x:ty ),+ ) => {{
        let mut iter = $string.split($sep);
        ($(iter.next().and_then(|word| word.parse::<$x>().ok()),)*)
    }}
}
const COLUMN_ID_SIZE: usize = 4;
const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;
const COLUMN_ID_OFFSET: usize = 0;
const COLUMN_USERNAME_OFFSET: usize = COLUMN_ID_SIZE;
const COLUMN_EMAIL_OFFSET: usize = COLUMN_USERNAME_OFFSET + COLUMN_USERNAME_SIZE;
const ROW_SIZE: usize = COLUMN_ID_SIZE + COLUMN_USERNAME_SIZE + COLUMN_EMAIL_SIZE;
const PAGE_SIZE: usize = 4096;
const MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const MAX_ROWS: usize = MAX_PAGES * ROWS_PER_PAGE;
#[derive(Debug)]
enum MetaCommandResult {
    Success,
    Unrecognized,
}

#[derive(Debug)]
enum PrepareResult {
    Success,
    SyntaxError,
    Unrecognized,
}

#[derive(Debug)]
enum ExecuteResult {
    None,
    Success,
    TableFull,
}

#[derive(Debug)]
enum StatementType {
    None,
    Insert,
    Select,
}

#[derive(Debug)]
#[repr(C)]
struct Row {
    id: u32,
    username: [u8; COLUMN_USERNAME_SIZE],
    email: [u8; COLUMN_EMAIL_SIZE]
}
impl Row {
    fn new(id: u32, username: String, email: String) -> Row {
        let mut row = Row {
            id,
            username: [0; COLUMN_USERNAME_SIZE],
            email: [0; COLUMN_EMAIL_SIZE]
        };
        let username_bytes = username.as_bytes();
        let email_bytes = email.as_bytes();

        row.username[..username_bytes.len()].copy_from_slice(username_bytes);
        row.email[..email_bytes.len()].copy_from_slice(email_bytes);
        row
    }
    fn print(&self) {
        if let Ok(username) = from_utf8(self.username.as_ref()) {
            if let Ok(email) = from_utf8(self.email.as_ref()) {
                println!("{}, {}, {}", self.id, username, email)
            }
        }
    }
}
fn serialize(row_src: &Option<Row>, row_dst: &mut [u8]) {
    unsafe {
        if let Some(row_src) = row_src {
            let id_ptr = &row_src.id as *const u32 as *const u8;
            ptr::copy_nonoverlapping(id_ptr, row_dst[COLUMN_ID_OFFSET..].as_mut_ptr() as *mut u8, COLUMN_ID_SIZE);
            let username_ptr = row_src.username.as_ptr() as *const u8;
            ptr::copy_nonoverlapping(username_ptr, row_dst[COLUMN_USERNAME_OFFSET..].as_mut_ptr() as *mut u8, COLUMN_USERNAME_SIZE);
            let email_ptr = row_src.email.as_ptr() as *const u8;
            ptr::copy_nonoverlapping(email_ptr, row_dst[COLUMN_EMAIL_OFFSET..].as_mut_ptr() as *mut u8, COLUMN_EMAIL_SIZE);
        }
    }
}
fn deserialize(row_src: &mut [u8], row_dst: &mut Option<Row>) {
    unsafe {
        if let Some(row_dst) = row_dst {
            let id_ptr = &mut row_dst.id as *mut u32 as *mut u8;
            ptr::copy_nonoverlapping(row_src[COLUMN_ID_OFFSET..].as_ptr() as *const u8, id_ptr, COLUMN_ID_SIZE);
            let username_ptr = row_dst.username.as_mut_ptr() as *mut u8;
            ptr::copy_nonoverlapping(row_src[COLUMN_USERNAME_OFFSET..].as_ptr() as *const u8, username_ptr, COLUMN_USERNAME_SIZE);
            let email_ptr = row_dst.email.as_mut_ptr() as *mut u8;
            ptr::copy_nonoverlapping(row_src[COLUMN_EMAIL_OFFSET..].as_ptr() as *const u8, email_ptr, COLUMN_EMAIL_SIZE);
        }
    }
}

#[derive(Debug)]
struct Table {
    num_rows: usize,
    pages: [Option<Box<[u8; PAGE_SIZE]>>; MAX_PAGES]
}

impl Table {
    fn new() -> Self {
        Self {
            num_rows: 0,
            pages: [(); MAX_PAGES].map(|_| None),
        }
    }
    fn row_slot(&mut self, index: usize) -> &mut [u8] {
        let page_num = index / ROWS_PER_PAGE;
        if page_num >= MAX_PAGES {
            panic!("Page number out of bounds");
        }

        // Allocate the page if it hasn't been allocated yet
        if self.pages[page_num].is_none() {
            self.pages[page_num] = Some(Box::new([0; PAGE_SIZE]));
        }

        let page = self.pages[page_num].as_mut().unwrap();
        let row_offset = index % ROWS_PER_PAGE;
        let byte_offset = row_offset * ROW_SIZE;

        &mut page[byte_offset..byte_offset + ROW_SIZE]
    }

}
#[derive(Debug)]
struct Statement {
    statement_type: StatementType,
    row_to_insert: Option<Row>
}

fn print_prompt() {
    print!("rsql > ");
    io::Write::flush(&mut io::stdout()).expect("flush failed!");
}

fn do_meta_command(buf: &mut String) -> MetaCommandResult {

    if buf == ".exit" {
        exit(0);
    }
    else {
        println!("Unrecognized Command {}.", buf);
        MetaCommandResult::Unrecognized
    }
}

fn prepare_statement(buf: &mut String, statement: &mut Statement) -> PrepareResult {
    if buf.starts_with("insert") {
        statement.statement_type = StatementType::Insert;
        let input = scan!(buf, char::is_whitespace, u32, String, String);
        if let (Some(id), Some(username), Some(email)) = input {
            statement.row_to_insert = Some(Row::new(id, username, email));
        }
        return PrepareResult::Success;
    }
    else if buf.starts_with("select") {
        statement.statement_type = StatementType::Select;
        return PrepareResult::Success;
    }
    PrepareResult::Unrecognized
}

fn execute_statement(table: &mut Table, statement: &mut Statement) -> ExecuteResult{
    match statement.statement_type {
        StatementType::Insert => {
            println!("inserting...");
            execute_insert(table, statement)
        }
        StatementType::Select => {
            println!("selecting...");
            execute_select(table, statement)
        }
        _ => {
            ExecuteResult::None
        }
    }
}
fn execute_insert(table: &mut Table, statement: &mut Statement) -> ExecuteResult {
    if table.num_rows >= MAX_ROWS {
        return ExecuteResult::TableFull;
    }
    serialize(&statement.row_to_insert, table.row_slot(table.num_rows));
    table.num_rows += 1;
    println!("{:?}", table);
    ExecuteResult::Success
}
fn execute_select(table: &mut Table, statement: &mut Statement) -> ExecuteResult {
    let mut row: Option<Row> = None;
    for i in 0..table.num_rows {
        deserialize(table.row_slot(i), &mut row);
        if let Some(row) = &row {
            println!("{:?}", row);
            row.print()
        }
    }
    ExecuteResult::Success
}

fn read_input(buf: &mut String) -> &String {
    match io::stdin().read_line(buf) {
        Ok(_bytes_read) => {
            let trimmed = buf.trim_end().to_string();
            buf.clear();
            buf.push_str(&trimmed);
            buf
        }
        Err(_e) => {
            println!("Error reading input");
            exit(1)
        }
    }
}

fn main() {
    let mut table: Box<Table> = Box::new(Table::new());
    let mut input_buffer: String = String::new();
    loop {
        input_buffer.clear();
        print_prompt();
        read_input(&mut input_buffer);
        if input_buffer[0..1] == *"." {
            match do_meta_command(&mut input_buffer) {
                MetaCommandResult::Success => {
                    continue;
                }
                MetaCommandResult::Unrecognized => {
                    println!("Unrecognized Command '{}'.", input_buffer);
                    continue;
                }
                _ => {
                    println!("Unhandled result, panic");
                    panic!();
                }
            }
        }
        let mut statement: Statement = Statement {statement_type: StatementType::None, row_to_insert: None};
        match prepare_statement(&mut input_buffer, &mut statement) {
            PrepareResult::Success => { }
            PrepareResult::SyntaxError => {
                println!("Syntax Error in '{}'", input_buffer);
                continue;
            }
            PrepareResult::Unrecognized => {
                println!("Unrecognized keyword at start of '{}'.", input_buffer);
                continue;
            }
            _ => {
                println!("Unhandled result, panic");
                panic!();
            }
        }
        match execute_statement(table.as_mut(), &mut statement) {
            ExecuteResult::Success => {}
            ExecuteResult::TableFull => {}
            _ => {
                println!("Unhandled result, panic");
                panic!();
            }
        }

    }
}
