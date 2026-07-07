#[tokio::main]
async fn main() {
    let result = crius::crs::run_cli(std::env::args_os()).await;
    std::process::exit(result.code());
}
