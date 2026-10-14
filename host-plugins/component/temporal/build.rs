//! Generates the gRPC client for the five WorkflowService calls the plugin
//! makes.
//!
//! `temporalio-protos` compiles Temporal's protos with protox, and hands this
//! script their descriptor set through its `links` metadata. The client is
//! generated from that set, with no tonic transport, and uses the message
//! types `temporalio-protos` already defines.

use std::{env, fs};

use prost::Message;
use tonic_prost_build::FileDescriptorSet;

const SERVICE_FILE: &str = "temporal/api/workflowservice/v1/service.proto";
const METHODS: &[&str] = &[
    "PollActivityTaskQueue",
    "RespondActivityTaskCompleted",
    "RespondActivityTaskFailed",
    "RespondActivityTaskCanceled",
    "RecordActivityTaskHeartbeat",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = env::var("DEP_TEMPORALIO_PROTOS_DESCRIPTOR_PATH")?;
    println!("cargo:rerun-if-changed={path}");

    let mut descriptors = FileDescriptorSet::decode(fs::read(&path)?.as_slice())?;
    descriptors.file.retain(|file| file.name() == SERVICE_FILE);
    for service in &mut descriptors.file[0].service {
        service
            .method
            .retain(|method| METHODS.contains(&method.name()));
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .build_transport(false)
        .extern_path(".temporal", "::temporalio_protos::temporal")
        .compile_fds(descriptors)?;
    Ok(())
}
