use pe_core_types::KellyFraction;

fn main() {
    // KellyFraction must not accept f64 literals via From/Into.
    let _p: KellyFraction = 0.25_f64;
}
