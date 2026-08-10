//! DEF-091-F — SDA lexer/parser must not panic on adversarial text (PR CI).

use proptest::prelude::*;
use residiuum_sda::Program;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    #[test]
    fn parse_never_panics(s in ".*{0,512}") {
        let _ = Program::parse(&s);
    }

    #[test]
    fn parse_binary_lossy_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        let s = String::from_utf8_lossy(&bytes);
        let _ = Program::parse(&s);
    }
}
