// SPDX-License-Identifier: Apache-2.0

/// Little-endian 64-bit unsigned integer.
#[derive(Wrapper, WrapperMut, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, From)]
#[wrapper(Deref, Display, FromStr, Octal, LowerHex, UpperHex, Add, Sub, Mul, Div, Rem, BitOps)]
#[wrapper_mut(DerefMut, AddAssign, SubAssign, MulAssign, DivAssign, RemAssign, BitAssign)]
pub struct U64Le(pub u64);
impl From<U64Le> for [u8; 8] {
    fn from(value: U64Le) -> Self { value.0.to_le_bytes() }
}
impl From<[u8; 8]> for U64Le {
    fn from(value: [u8; 8]) -> Self { Self(u64::from_le_bytes(value)) }
}

/// Big-endian 64-bit unsigned integer.
#[derive(Wrapper, WrapperMut, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, From)]
#[wrapper(Deref, Display, FromStr, Octal, LowerHex, UpperHex, Add, Sub, Mul, Div, Rem, BitOps)]
#[wrapper_mut(DerefMut, AddAssign, SubAssign, MulAssign, DivAssign, RemAssign, BitAssign)]
pub struct U64Be(pub u64);
impl From<U64Be> for [u8; 8] {
    fn from(value: U64Be) -> Self { value.0.to_be_bytes() }
}
impl From<[u8; 8]> for U64Be {
    fn from(value: [u8; 8]) -> Self { Self(u64::from_be_bytes(value)) }
}
