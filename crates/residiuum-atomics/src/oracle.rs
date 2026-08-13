//! Serial in-memory oracle boundary (`ATOMICS_IMPLEMENTATION_PLAN` §6).
//!
//! The deliberately slow oracle and the history format it consumes land in
//! ATM-0.5. This module exists so later cards do not invent a second home.

/// Marker for the ATM-0.5 serial oracle. No evaluation API yet.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SerialOracle;

impl SerialOracle {
    /// Reserved constructor. Evaluation is ATM-0.5.
    pub const fn new() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_boundary_exists() {
        let _ = SerialOracle::new();
    }
}
