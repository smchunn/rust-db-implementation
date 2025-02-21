// tests/db_tests.rs

use rsql::{Row, Table, serialize, deserialize, ROW_SIZE, run_repl};
use std::io::{BufReader, Cursor};

#[cfg(test)]
mod tests {
    use super::*;

    // DB tests
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
    fn test_table_insertion() {
        let mut table = Table::new();
        let row = Row::new(1, "testuser".to_string(), "test@example.com".to_string());

        serialize(&row, table.row_slot(0));
        table.num_rows += 1;

        assert_eq!(table.num_rows, 1);
        let deserialized_row = deserialize(table.row_slot(0));
        assert_eq!(deserialized_row.id, 1);
        assert_eq!(&deserialized_row.username[..8], b"testuser");
    }

    #[test]
    fn test_multiple_rows() {
        let mut table = Table::new();

        let row1 = Row::new(1, "user1".to_string(), "user1@example.com".to_string());
        let row2 = Row::new(2, "user2".to_string(), "user2@example.com".to_string());

        serialize(&row1, table.row_slot(0));
        table.num_rows += 1;
        serialize(&row2, table.row_slot(1));
        table.num_rows += 1;

        let deserialized_row1 = deserialize(table.row_slot(0));
        let deserialized_row2 = deserialize(table.row_slot(1));

        assert_eq!(table.num_rows, 2);
        assert_eq!(deserialized_row1.id, 1);
        assert_eq!(deserialized_row2.id, 2);
    }

    // REPL tests
    #[test]
    fn test_repl_insert_and_select() {
        let mut table = Table::new();

        let x = 5;
        // Simulate stdin with a Cursor
        let mut input = String::new();
        for i in 0..x {
            input.push_str(&format!("insert {} user{} user{}@example.com\n", i, i, i));
        }
        input.push_str("select\n"); // Add newline for select
        input.push_str(".exit\n");  // Add exit to terminate REPL

        let mut reader = BufReader::new(Cursor::new(input));

        // Capture stdout in a Vec<u8>
        let mut output = Vec::new();

        // Run the REPL
        run_repl(&mut table, &mut reader, &mut output);

        // Build expected output
        let mut expected = String::new();
        for _i in 0..x {
            expected.push_str("rsql > "); // Prompt for each insert
        }
        expected.push_str("rsql > "); // Prompt for select
        for i in 0..x {
            expected.push_str(&format!("{} user{} user{}@example.com\n", i, i, i));
        }
        expected.push_str("rsql > "); // Prompt before .exit

        // Convert output to string for assertion
        let output_str = String::from_utf8(output).unwrap();

        // Assert the expected output
        assert_eq!(output_str, expected);
    }

    #[test]
    fn test_repl_insert_and_select_exceed_max_rows() {
        let mut table = Table::new();

        let x = rsql::MAX_ROWS + 1;
        // Simulate stdin with a Cursor
        let mut input = String::new();
        for i in 0..x {
            input.push_str(&format!("insert {} user{} user{}@example.com\n", i, i, i));
        }
        input.push_str(".exit\n");  // Add exit to terminate REPL

        let mut reader = BufReader::new(Cursor::new(input));

        // Capture stdout in a Vec<u8>
        let mut output = Vec::new();

        // Run the REPL
        run_repl(&mut table, &mut reader, &mut output);

        // Build expected output
        let mut expected = String::new();
        for i in 0..x {
            expected.push_str("rsql > "); // Prompt for each insert
            if i >= rsql::MAX_ROWS {
                expected.push_str(&format!("Row not inserted, table full '{} user{} user{}@example.com'\n", i, i, i));
            }
        }
        expected.push_str("rsql > "); // Prompt before .exit

        // Convert output to string for assertion
        let output_str = String::from_utf8(output).unwrap();

        // Assert the expected output
        assert_eq!(output_str, expected);
    }

    #[test]
    fn test_repl_invalid_command() {
        let mut table = Table::new();

        let input = "invalid command\n.exit\n";
        let mut reader = BufReader::new(Cursor::new(input));
        let mut output = Vec::new();

        run_repl(&mut table, &mut reader, &mut output);

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("Unrecognized keyword at start of 'invalid command'"));
    }

    #[test]
    fn test_repl_syntax_error() {
        let mut table = Table::new();

        let input = "insert 1\n.exit\n";  // Incomplete insert command
        let mut reader = BufReader::new(Cursor::new(input));
        let mut output = Vec::new();

        run_repl(&mut table, &mut reader, &mut output);

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("Syntax Error in 'insert 1'"));
    }
}
