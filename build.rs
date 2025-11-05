fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/proto")
        .compile(
            &["proto/k8s.io/cri-api/pkg/apis/runtime/v1alpha2/api.proto"],
            &["proto"],
        )?;
    Ok(())
}
