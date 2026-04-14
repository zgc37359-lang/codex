fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::compile_protos("src/remote/proto/codex.thread_store.v1.proto")?;
    Ok(())
}
