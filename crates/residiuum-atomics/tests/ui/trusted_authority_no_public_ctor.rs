fn main() {
    let heap = residiuum_atomics::HeapId::from_bytes([1u8; 16]).unwrap();
    let _ = residiuum_atomics::TrustedAuthorityView::new(heap, [2u8; 32]);
}
