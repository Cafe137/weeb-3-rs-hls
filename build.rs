fn main() {
    prost_build::compile_protos(
        &[
            "src/etiquette_0.proto",
            "src/etiquette_1.proto",
            "src/etiquette_2.proto",
            "src/etiquette_4.proto",
            "src/etiquette_5.proto",
            "src/etiquette_6.proto",
            "src/etiquette_7.proto",
            "src/etiquette_8.proto",
        ],
        &["src/"],
    )
    .unwrap();
}
