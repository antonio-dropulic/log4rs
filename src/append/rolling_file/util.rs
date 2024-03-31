/// replace pattern occurences in the src string with count
/// produce a String with replaced values, and an index map to all the replaced
/// values start. This allows us to perform [increment_count].
pub fn replace_count(src: &str, pattern: &str, count: u32) -> (Vec<usize>, String) {
    let mut result = String::new();
    let mut replaced_idx = Vec::new();

    let count = count.to_string();
    let idx_left_shift = pattern.len() - count.len();

    let mut last_end = 0;
    for (num_replaced, (pattern_start, part)) in src.match_indices(pattern).enumerate() {
        result.push_str(unsafe { src.get_unchecked(last_end..pattern_start) });
        result.push_str(&count);
        last_end = pattern_start + part.len();
        replaced_idx.push(pattern_start - num_replaced * idx_left_shift);
    }
    result.push_str(unsafe { src.get_unchecked(last_end..src.len()) });
    (replaced_idx, result)
}

pub fn increment_count(
    mut src: String,
    current_count: u32,
    count_idxs: &[usize],
) -> (Vec<usize>, String) {
    let incremented_count = (current_count + 1).to_string();
    let current_count = current_count.to_string();

    let left_shift = current_count.len() - incremented_count.len();
    let mut new_count_idx = Vec::new();

    for (num_replacements, start_idx) in count_idxs.iter().enumerate() {
        let start_idx = start_idx - num_replacements * left_shift;
        let end_idx = start_idx + current_count.len();
        // SAFETY:
        // count idxs is made when creating the file_name with replace_count
        // There is no other way to change it
        src.replace_range(start_idx..end_idx, &incremented_count);
        new_count_idx.push(start_idx)
    }

    (new_count_idx, src)
}
