use super::{Memory, Range, Scan, Store};
use crate::error::Result;

use std::fmt::Display;
use std::sync::{Arc, RwLock};
use num_format::{Locale, ToFormattedString};

const MAX_ORDER: u32 = 4096;
const MAX_PAGE_SIZE: u32 = 8192;

/// Key-value storage backend for testing. Protects an inner Memory backend using a mutex, so it can
/// be cloned and inspected.
#[derive(Clone)]
pub struct Test {
    kv: Arc<RwLock<Memory>>,
}

impl Test {
    /// Creates a new Test key-value storage engine.
    pub fn new() -> Self {
        Self { kv: Arc::new(RwLock::new(Memory::new())) }
    }
}

impl Display for Test {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "test")
    }
}

impl Store for Test {
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.kv.write()?.delete(key)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.kv.read()?.get(key)
    }

    fn scan(&self, range: Range) -> Scan {
        // Since the mutex guard is scoped to this method, we simply buffer the result.
        Box::new(self.kv.read().unwrap().scan(range).collect::<Vec<Result<_>>>().into_iter())
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.kv.write()?.set(key, value)
    }
}

#[cfg(test)]
impl super::TestSuite<Test> for Test {
    fn setup() -> Result<Self> {
        Ok(Test::new())
    }
}

#[test]
fn tests() -> Result<()> {
    use super::TestSuite;
    Test::test()
}

fn calculate_efficiency(page_size: u32, optimum_page_size: u32, value: u32, records: u64) {
    let percentage = page_size as f64 / optimum_page_size as f64;
    println!("{}", page_size);
    println!("{}", optimum_page_size);
    let num_pages = records as f64 / value as f64;
    let total_space = num_pages * page_size as f64;
    let wasted_space = (1.0 - percentage) * total_space;

    let total_space_gb = total_space / 1024.0 / 1024.0 / 1024.0;
    let mut wasted_space_gb = wasted_space / 1024.0 / 1024.0 / 1024.0;

    if wasted_space_gb < 0.0 {
        wasted_space_gb = 0.0;
    }

    println!("# Records: {}, \
              Total: {:.2} GB, \
              Wasted: {:.2} GB, \
              Efficiency: {:.2}",
             records.to_formatted_string(&Locale::en),
             total_space_gb,
             wasted_space_gb,
             percentage)
}

fn calculate_page_size(
    max_order: u32,
    header_size: u32,
    key_size: u32,
    file_offset_size: u32,
    page_offset_size: u32) -> u32 {
    let d = max_order;

    let page_size = key_size * (d - 1) +
        (file_offset_size * d) +
        (page_offset_size * d) + header_size;

    return page_size as u32;
}

fn calculate_max_order(
    page_size: u32,
    header_size: u32,
    key_size: u32,
    file_offset_size: u32,
    page_offset_size: u32) -> u32 {
    let usable_space = page_size - header_size;
    // Get the number down to a realm where the calculations are quicker.
    let mut d = ((page_size / (key_size + file_offset_size + page_offset_size)) / 4) * 3;

    loop {
        let possible_space =
            key_size * (d - 1) +
                (file_offset_size * d) +
                (page_offset_size * d);

        if possible_space >= usable_space {
            let max_d = d - 1;
            let computed_space =
                key_size * (max_d - 1) +
                    file_offset_size *
                        max_d + page_offset_size * max_d;

            println!("Space Available: {}, \
                      Space Used: {}, \
                      Elements Possible: {}, \
                      Unusable Space: {}, \
                      Total Element Size: {}",
                     usable_space,
                     key_size * (max_d - 1) + file_offset_size * max_d,
                     max_d,
                     usable_space - computed_space,
                     key_size +
                         file_offset_size +
                         page_offset_size);

            return max_d;
        }

        d = d + 1;
    }
}

#[test]
fn test_page_efficiencies() {
    let optimum = calculate_max_order(MAX_PAGE_SIZE, 64, 16, 4, 0);
    let minimum = calculate_max_order(MAX_PAGE_SIZE, 64, 16, 4, 2);
    let minimum2 = calculate_max_order(MAX_PAGE_SIZE, 64, 16, 4, 3);
    let maximum = calculate_max_order(MAX_PAGE_SIZE, 64, 16, 8, 4);
    let maximum2 = calculate_max_order(MAX_PAGE_SIZE, 64, 16, 8, 2);

    println!("Minimum:");
    calculate_efficiency(minimum, optimum, 4096, 100000000);
    calculate_efficiency(minimum, optimum, 4096, 1000000000);
    calculate_efficiency(minimum, optimum, 4096, 10000000000);

    println!("Minimum2:");
    calculate_efficiency(minimum2, optimum, 4096, 100_000_000);
    calculate_efficiency(minimum2, optimum, 4096, 1_000_000_000);
    calculate_efficiency(minimum2, optimum, 4096, 10_000_000_000);
    calculate_efficiency(minimum2, optimum, 4096, 100_000_000_000);

    println!("Maximum:");
    calculate_efficiency(maximum, optimum, 4096, 100000000);
    calculate_efficiency(maximum, optimum, 4096, 1000000000);
    calculate_efficiency(maximum, optimum, 4096, 10000000000);

    println!("Maximum2:");
    calculate_efficiency(maximum2, optimum, 4096, 100000000);
    calculate_efficiency(maximum2, optimum, 4096, 1000000000);
    calculate_efficiency(maximum2, optimum, 4096, 10000000000);


    let page_size = calculate_page_size(MAX_ORDER, 64, 16, 4, 4);
    let optimum3 = calculate_page_size(MAX_ORDER, 64, 16, 8, 0);

    println!("Page Size: {}", page_size);
    println!("Efficiency:");
    calculate_efficiency(page_size, optimum3, 4096, 100000000);
    calculate_efficiency(page_size, optimum3, 4096, 1000000000);
    calculate_efficiency(page_size, optimum3, 4096, 10000000000);
}