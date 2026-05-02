use pe_core_types::Probability;

fn main() {
    // Probability must not accept f64 literals via From/Into.
    let _p: Probability = 0.5_f64;
}
