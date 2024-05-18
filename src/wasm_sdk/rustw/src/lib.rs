use std::io::Write;

use build_html::{Html, HtmlContainer};

extern "C" {
    pub fn call(cmd: *const u8);
}

static mut BUFFER: Option<String> = None;

pub fn run(command: &[&str]) -> serde_json::Value {
    let mut bytes = Vec::<u8>::new();

    let res = unsafe {
        bytes
            .write(&std::mem::transmute::<i32, [u8; 4]>(command.len() as i32))
            .unwrap();

        for part in command {
            bytes
                .write(&std::mem::transmute::<i32, [u8; 4]>(part.len() as i32))
                .unwrap();
            bytes.write(part.as_bytes()).unwrap();
        }

        call(bytes.as_ptr());

        BUFFER.take().unwrap()
    };

    println!("GOT! {}", res);
    serde_json::from_str(&res).unwrap()
}

#[no_mangle]
pub unsafe fn provide_buffer(bytes: i32) -> *mut u8 {
    BUFFER.insert(str::repeat(" ", bytes as usize)).as_mut_ptr()
}

pub unsafe fn leak_string(s: String) -> *const u8{
    let len = s.len();
    let ptr = s.leak().as_mut_ptr();
    *ptr.wrapping_add(len) = b'\0';
    ptr
}

#[no_mangle]
pub fn my_fun() -> *const u8 {
    let titles: Vec<String> = run(&["LRANGE", "TITLES", "0", "-1"])
        .get("result")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_owned())
        .collect();

    let list_items = titles.into_iter()
         .map(|t| format!("Title item {}", t))
         .fold(build_html::Container::new(build_html::ContainerType::OrderedList), |a, n| a.with_paragraph(n));

    let page = build_html::HtmlPage::new()
        .with_title("MY ENTRIES")
        .with_container(build_html::Container::default()
            .with_container(list_items)
        ).to_html_string();
    return unsafe { leak_string(page) };
}
