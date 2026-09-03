use std::fmt::Display;

use crate::error::{Error, Result};
use crate::internal_err;

#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum JoinType {
    Left,
    Right,
    Inner,
    Full,
    LeftSemi,
    LeftAnti,
}

impl Display for JoinType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Left => write!(f, "Left Join"),
            JoinType::Right => write!(f, "Right Join"),
            JoinType::Inner => write!(f, "Inner Join"),
            JoinType::Full => write!(f, "Full Join"),
            JoinType::LeftSemi => write!(f, "Left Semi Join"),
            JoinType::LeftAnti => write!(f, "Left Anti Join"),
        }
    }
}

/// `CROSS` has no equivalent here -- it is a `CrossJoin` plan node rather than a join type -- so
/// the conversion is fallible and callers have to have dealt with it already.
impl TryFrom<sqlparser::ast::JoinType> for JoinType {
    type Error = crate::error::Error;

    fn try_from(value: sqlparser::ast::JoinType) -> Result<Self> {
        match value {
            sqlparser::ast::JoinType::Inner => Ok(JoinType::Inner),
            sqlparser::ast::JoinType::Left => Ok(JoinType::Left),
            sqlparser::ast::JoinType::Right => Ok(JoinType::Right),
            sqlparser::ast::JoinType::Full => Ok(JoinType::Full),
            sqlparser::ast::JoinType::Cross => internal_err!("CROSS JOIN has no JoinType; it is a CrossJoin plan"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cross_join_has_no_join_type() {
        assert_eq!(
            JoinType::try_from(sqlparser::ast::JoinType::Inner).unwrap(),
            JoinType::Inner
        );
        assert_eq!(
            JoinType::try_from(sqlparser::ast::JoinType::Left).unwrap(),
            JoinType::Left
        );
        assert_eq!(
            JoinType::try_from(sqlparser::ast::JoinType::Right).unwrap(),
            JoinType::Right
        );
        assert_eq!(
            JoinType::try_from(sqlparser::ast::JoinType::Full).unwrap(),
            JoinType::Full
        );

        // CROSS is a plan node, not a join type; converting it must not panic.
        let err = JoinType::try_from(sqlparser::ast::JoinType::Cross).unwrap_err();
        assert!(err.to_string().contains("CROSS JOIN"), "unexpected error: {err}");
    }
}
