fn main() {
    fn elevate(view: &mut residiuum_atomics::TrustedAuthorityView) {
        let cid = residiuum_atomics::CollectionId::from_bytes([1u8; 16]).unwrap();
        view.grant(cid, residiuum_atomics::CollectionRights::ordinary());
    }
    let _ = elevate;
}
