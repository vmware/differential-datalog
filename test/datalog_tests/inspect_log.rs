use ::std::fs::OpenOptions;
use ::std::io::Write;

/*
 * Logs the given 'msg' to a file 'filename'.
 */
pub fn log(filename: &String, msg: &String) {
    let mut file = OpenOptions::new().append(true).create(true).open(filename).unwrap();
    file.write_all((msg.to_owned() + "\n").as_bytes()).unwrap();
    ()
}
