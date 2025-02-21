use std::{io::{self, Write}, mem::MaybeUninit, process::exit, ptr, str::from_utf8};

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
    Success,
    TableFull,
}

#[derive(Debug)]
enum StatementType {
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
        let username = from_utf8(&self.username).unwrap_or("Invalid UTF-8");
        let email = from_utf8(&self.email).unwrap_or("Invalid UTF-8");
        println!("{} {} {}", self.id, username.trim_end_matches('\0'), email.trim_end_matches('\0'));
    }
}

fn serialize(row_src: &Row, row_dst: &mut [u8]) {
    unsafe {
        ptr::copy_nonoverlapping(
            &row_src.id as *const u32 as *const u8,
            row_dst.as_mut_ptr().add(COLUMN_ID_OFFSET),
            COLUMN_ID_SIZE
        );
        ptr::copy_nonoverlapping(
            row_src.username.as_ptr(),
            row_dst.as_mut_ptr().add(COLUMN_USERNAME_OFFSET),
            COLUMN_USERNAME_SIZE
        );
        ptr::copy_nonoverlapping(
            row_src.email.as_ptr(),
            row_dst.as_mut_ptr().add(COLUMN_EMAIL_OFFSET),
            COLUMN_EMAIL_SIZE
        );
    }
}

fn deserialize(row_src: &[u8]) -> Row {
    let mut row = Row {
        id: 0,
        username: [0; COLUMN_USERNAME_SIZE],
        email: [0; COLUMN_EMAIL_SIZE]
    };
    unsafe {
        ptr::copy_nonoverlapping(
            row_src.as_ptr().add(COLUMN_ID_OFFSET),
            &mut row.id as *mut u32 as *mut u8,
            COLUMN_ID_SIZE
        );
        ptr::copy_nonoverlapping(
            row_src.as_ptr().add(COLUMN_USERNAME_OFFSET),
            row.username.as_mut_ptr(),
            COLUMN_USERNAME_SIZE
        );
        ptr::copy_nonoverlapping(
            row_src.as_ptr().add(COLUMN_EMAIL_OFFSET),
            row.email.as_mut_ptr(),
            COLUMN_EMAIL_SIZE
        );
    }
    row
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
    let _ = io::stdout().flush();
}

fn do_meta_command(buf: &str) -> MetaCommandResult {
    if buf == ".exit" {
        exit(0);
    }
    println!("Unrecognized Command '{}'.", buf);
    MetaCommandResult::Unrecognized
}

fn prepare_statement(buf: &str, statement: &mut Statement) -> PrepareResult {
    if buf.starts_with("insert") {
        statement.statement_type = StatementType::Insert;
        let input = scan!(buf, char::is_whitespace, String, u32, String, String);

        if let (Some(_insert), Some(id), Some(username), Some(email)) = input {
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

fn execute_statement(table: &mut Table, statement: &Statement) -> ExecuteResult {
    match statement.statement_type {
        StatementType::Insert => {
            if let Some(row) = &statement.row_to_insert {
                execute_insert(table, row)
            } else {
                ExecuteResult::Success
            }
        }
        StatementType::Select => execute_select(table),
    }
}

fn execute_insert(table: &mut Table, row: &Row) -> ExecuteResult {
    if table.num_rows >= MAX_ROWS {
        return ExecuteResult::TableFull;
    }
    serialize(row, table.row_slot(table.num_rows));
    table.num_rows += 1;
    ExecuteResult::Success
}

fn execute_select(table: &mut Table) -> ExecuteResult {
    for i in 0..table.num_rows {
        let row = deserialize(table.row_slot(i));
        row.print();
    }
    ExecuteResult::Success
}

fn main() {
    let mut table = Table::new();
    let mut input_buffer = String::new();

    loop {
        print_prompt();
        input_buffer.clear();
        io::stdin().read_line(&mut input_buffer).expect("Failed to read input");
        let input = input_buffer.trim();

        if input.starts_with('.') {
            do_meta_command(input);
            continue;
        }

        let mut statement = Statement {
            statement_type: StatementType::Insert,  // Default value, will be overridden
            row_to_insert: None
        };

        match prepare_statement(input, &mut statement) {
            PrepareResult::Success => {
                execute_statement(&mut table, &statement);
            }
            PrepareResult::SyntaxError => println!("Syntax Error in '{}'", input),
            PrepareResult::Unrecognized => println!("Unrecognized keyword at start of '{}'", input),
        }
    }
}
