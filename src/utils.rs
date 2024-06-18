use std::ops::Range;

pub fn split_range(range: Range<u32>, chunk_size: u16) -> Vec<Range<u32>> {
    let end = range.end;

    range
        .step_by(chunk_size as usize)
        .map(|start| {
            let end = (start + (chunk_size as u32)).min(end);
            start..end
        })
        .collect()
}
