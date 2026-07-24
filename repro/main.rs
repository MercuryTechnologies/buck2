use ring::digest;

fn main() {
    let d = digest::digest(&digest::SHA256, b"hello");
    println!("{:?}", d);
}
