//! CR-R2-004: public consumers cannot mint or elevate trusted grants.

#[test]
fn trusted_authority_not_forgeable() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/trusted_authority_no_public_ctor.rs");
    t.compile_fail("tests/ui/trusted_authority_no_public_grant.rs");
    t.compile_fail("tests/ui/trusted_authority_no_struct_literal.rs");
}
