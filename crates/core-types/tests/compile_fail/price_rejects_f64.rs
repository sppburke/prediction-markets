use pe_core_types::Price;

fn main() {
    // Price must not accept f64 literals via From/Into.
    let _p: Price = 0.5_f64;
}
