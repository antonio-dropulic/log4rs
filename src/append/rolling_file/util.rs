pub fn replace_count<'a>(src: &'a str, from_pattern: &str, count: u32) -> (Vec<usize>, String) {
    let mut result = String::new();
    let mut replaced_idx = Vec::new();
    let mut count_replaced = 0;

    let to_string = count.to_string();
    let idx_left_shift = from_pattern.len() - to_string.len();

    let mut last_end = 0;
    for (start, part) in src.match_indices(from_pattern) {
        result.push_str(unsafe { src.get_unchecked(last_end..start) });
        result.push_str(&to_string);
        last_end = start + part.len();
        replaced_idx.push(start - count_replaced * idx_left_shift);
        count_replaced += 1;
    }
    result.push_str(unsafe { src.get_unchecked(last_end..src.len()) });
    (replaced_idx, result)
}

pub fn increment_count(mut src: String, count: u32, count_idxs: &[usize]) -> (Vec<usize>, String) {
    let count_str = count.to_string();
    let new_count_str = (count + 1).to_string();

    let left_shift = count_str.len() - new_count_str.len();
    let mut new_count_idx = Vec::new();

    for (num_replacements, start_idx) in count_idxs.into_iter().enumerate() {
        let start_idx = start_idx - num_replacements * left_shift;
        let end_idx = start_idx + count_str.len();
        // SAFETY:
        // count idxs is made when creating the file_name with replace_count
        // There is no other way to change it
        src.replace_range(start_idx..end_idx, &new_count_str);
        new_count_idx.push(start_idx)
    }

    (new_count_idx, src)
}
