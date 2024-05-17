use std::io::Write;

extern "C" {
    pub fn call(cmd: *const u8);
}

static mut BUFFER: Option<String> = None;

pub fn run(command: &[&str]) -> String {
    let mut bytes = Vec::<u8>::new();

    unsafe {
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
    }
}

#[no_mangle]
pub unsafe fn provide_buffer(bytes: i32) -> *mut u8 {
    BUFFER.insert(str::repeat(" ", bytes as usize)).as_mut_ptr()
}

#[no_mangle]
pub fn my_fun() {
    let res = run(&["GET", "A"]);
    println!("Got {}", res);
}
