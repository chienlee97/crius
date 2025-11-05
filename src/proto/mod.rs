pub mod runtime {
    pub mod v1alpha2 {
        include!(concat!(env!("OUT_DIR"), "/runtime.v1alpha2.rs"));
    }
}
