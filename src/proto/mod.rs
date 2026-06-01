pub mod runtime {
    #[allow(clippy::doc_lazy_continuation)]
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/runtime.v1.rs"));
    }
}

pub mod nri {
    include!(concat!(env!("OUT_DIR"), "/nri/mod.rs"));
}
