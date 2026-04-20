fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = std::env::var("OUT_DIR")?;
    let descriptor_path = format!("{}/file_descriptor_set.bin", out_dir);
    let nri_out_dir = format!("{}/nri", out_dir);

    let mut config = prost_build::Config::new();
    config.file_descriptor_set_path(&descriptor_path);

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir("src/proto")
        .compile_with_config(
            config,
            &["proto/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto"],
            &["proto"],
        )?;
    let _ = std::fs::copy(descriptor_path, "src/proto/file_descriptor_set.bin");

    std::fs::create_dir_all(&nri_out_dir)?;
    ttrpc_codegen::Codegen::new()
        .out_dir(&nri_out_dir)
        .inputs(["proto/nri/pkg/api/api.proto"])
        .include("proto")
        .rust_protobuf()
        .customize(ttrpc_codegen::Customize {
            async_all: true,
            gen_mod: true,
            ..Default::default()
        })
        .rust_protobuf_customize(ttrpc_codegen::ProtobufCustomize::default().gen_mod_rs(false))
        .run()?;

    println!("cargo:rerun-if-changed=proto/nri/pkg/api/api.proto");
    println!("cargo:rerun-if-changed=proto/k8s.io/cri-api/pkg/apis/runtime/v1/api.proto");

    Ok(())
}
