mod cross_join;
mod nest_loop_join;

pub use cross_join::CrossJoin;
pub use nest_loop_join::*;

use crate::common::join_type::JoinType;

pub(crate) fn need_produce_result_in_final(join_type: &JoinType) -> bool {
    return join_type == &JoinType::Left || join_type == &JoinType::Full || join_type == &JoinType::Right;
}
