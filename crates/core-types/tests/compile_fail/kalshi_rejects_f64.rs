use pe_core_types::KalshiPriceCents;

fn main() {
    // KalshiPriceCents must not accept f64 literals via From/Into.
    let _p: KalshiPriceCents = 50.0_f64;
}
