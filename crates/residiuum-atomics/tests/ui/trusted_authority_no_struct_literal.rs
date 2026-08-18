fn main() {
    let _view = residiuum_atomics::TrustedAuthorityView {
        heap_id: residiuum_atomics::HeapId::from_bytes([1u8; 16]).unwrap(),
        revision: [2u8; 32],
        grants: Default::default(),
    };
}
