fn main() {
    #[cfg(feature = "enable-tvm")]
    link_tvm();
}

#[cfg(feature = "enable-tvm")]
fn link_tvm() {
    use std::env;
    use std::path::Path;
    use std::process::Command;

    let out_dir = env::var("OUT_DIR").unwrap();
    let out_dir = Path::new(&out_dir).join("test_nn");

    std::fs::create_dir_all(&out_dir).unwrap();

    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let manifest_dir = Path::new(&manifest_dir);
    let generator = manifest_dir.join("src").join("build_model_lib.py");
    let graph_path = out_dir.join("graph.o");

    let output = Command::new(&generator)
        .arg(&graph_path)
        .output()
        .expect("Failed to execute command");

    assert!(
        graph_path.exists(),
        "Graph file '{}' does not exist. Stderr: {} Stdout: {}",
        graph_path.display(),
        String::from_utf8(output.stderr)
            .unwrap()
            .trim()
            .split('\n')
            .last()
            .unwrap_or(""),
        String::from_utf8(output.stdout)
            .unwrap()
            .trim()
            .split('\n')
            .last()
            .unwrap_or("")
    );

    let lib_name = "data_pods_nn";
    let lib_file = out_dir.join(format!("lib{}.a", &lib_name));

    let sysroot_output = Command::new("rustc")
        .args(&["--print", "sysroot"])
        .output()
        .expect("Failed to get sysroot");

    let sysroot = String::from_utf8(sysroot_output.stdout).unwrap();
    let sysroot = sysroot.trim();
    let mut llvm_tools_path = std::path::PathBuf::from(&sysroot);
    llvm_tools_path.push("lib/rustlib/x86_64-unknown-linux-gnu/bin");

    Command::new("rustup")
        .args(&["component", "add", "llvm-tools-preview"])
        .output()
        .expect("failed to install llvm tools");

    if std::env::var("CARGO_CFG_TARGET_VENDOR").unwrap() == "fortanix" {
        std::process::Command::new(llvm_tools_path.join("llvm-objcopy"))
            .arg("--globalize-symbol=__tvm_module_startup")
            .arg("--remove-section=.ctors")
            .arg(graph_path.clone())
            .output()
            .expect("Could not globalize startup function");
    }

    std::process::Command::new(llvm_tools_path.join("llvm-ar"))
        .arg("rcs")
        .arg(lib_file)
        .arg(graph_path)
        .output()
        .expect("failed to package model archive");

    println!("cargo:rustc-link-lib=static={}", lib_name);

    println!("cargo:rustc-link-search=native={}", out_dir.display());
}
