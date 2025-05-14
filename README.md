# Build
cargo install wasm-bindgen-cli
RUSTFLAGS='--cfg getrandom_backend="wasm_js"' wasm-pack build --target web