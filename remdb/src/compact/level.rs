#![allow(unused)]

use std::collections::HashSet;

use itertools::Itertools;

use crate::core::Core;

#[derive(Debug, Clone)]
pub struct LevelsController {
    /// if the num of level 0 sstable is greater than `l0_limit`,
    /// then we need to compact level 0
    l0_limit: usize,

    /// max levels of db
    max_levels: usize,

    base_level_size_mb: u64,

    /// if `level_size_multiplier` is 10, then we have
    ///
    /// l0: 0mb <- l1: 10mb <- l2: 100mb <- l3: 1gb <- l4: 10gb
    level_size_multiplier: u64,
}

impl LevelsController {
    pub fn new(
        l0_limit: usize,
        max_levels: usize,
        base_level_size_mb: u64,
        level_size_multiplier: u64,
    ) -> Self {
        Self {
            l0_limit,
            max_levels,
            base_level_size_mb,
            level_size_multiplier,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LevelsTask {
    pub(crate) upper_level: usize,
    pub(crate) upper_level_ids: Vec<u32>,

    pub(crate) lower_level: usize,
    pub(crate) lower_level_ids: Vec<u32>,

    pub(crate) lower_level_bottom_level: bool,
}

impl LevelsController {
    pub fn generate_task(&self, core: &Core) -> Option<LevelsTask> {
        tracing::info!("Start generate leveled compaction task");

        let mut current_level_size = (1..=self.max_levels)
            .map(|level| core.get_level_size(level))
            .collect_vec(); // without level 0
        let base_level_size_bytes = self.base_level_size_mb * 1024 * 1024;

        let mut target_level_size = vec![0; self.max_levels];
        target_level_size[self.max_levels - 1] =
            current_level_size[self.max_levels - 1].max(base_level_size_bytes);
        let mut merge_base_level = self.max_levels;
        for i in (0..self.max_levels - 1).rev() {
            let next_level_size = target_level_size[i + 1];
            let cur_level_size = next_level_size / self.level_size_multiplier;
            if next_level_size > base_level_size_bytes {
                target_level_size[i] = cur_level_size;
            }
            if target_level_size[i] > 0 {
                merge_base_level = i + 1;
            }
        }

        // Flush l0 to target level
        if core.ssts[0].len() >= self.l0_limit {
            tracing::info!("Level 0 compaction task");
            let overlap = find_merge_overlapping_ssts(core, &core.ssts[0], merge_base_level);
            let task = LevelsTask {
                upper_level: 0,
                upper_level_ids: core.ssts[0].clone(),
                lower_level: merge_base_level,
                lower_level_ids: overlap,
                lower_level_bottom_level: merge_base_level == self.max_levels,
            };
            return Some(task);
        }

        let mut priorities = Vec::with_capacity(self.max_levels);
        for level in 1..=self.max_levels {
            if target_level_size[level - 1] == 0 {
                continue;
            }
            let priority =
                current_level_size[level - 1] as f64 / target_level_size[level - 1] as f64;
            if priority > 1.0 {
                priorities.push((priority, level));
            }
        }
        priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());

        tracing::debug!("merge priorities: {:?}", priorities);
        if let Some((_, level)) = priorities.first() {
            let level = *level;
            let selected_ssts = core.ssts[level].iter().min().copied().unwrap();
            let task = LevelsTask {
                upper_level: level,
                upper_level_ids: vec![selected_ssts],
                lower_level: level + 1,
                lower_level_ids: find_merge_overlapping_ssts(core, &[selected_ssts], level + 1),
                lower_level_bottom_level: level + 1 == self.max_levels,
            };
            return Some(task);
        }
        None
    }

    pub fn apply_compaction_result(&self, core: &Core, task: &LevelsTask) -> (Core, Vec<u32>) {
        let mut result = core.clone();
        let mut files_to_remove = Vec::new();
        let mut upper_level_ids_set = task.upper_level_ids.iter().copied().collect::<HashSet<_>>();
        let mut lower_level_ids_set = task.lower_level_ids.iter().copied().collect::<HashSet<_>>();
        if task.upper_level == 0 {
            let new_l0_ssts = result.ssts[0]
                .iter()
                .filter_map(|x| {
                    if upper_level_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            assert!(upper_level_ids_set.is_empty());
            result.ssts[0] = new_l0_ssts;
        } else {
            let new_upper_level_ssts = result.ssts[task.upper_level - 1]
                .iter()
                .filter_map(|x| {
                    if upper_level_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            assert!(upper_level_ids_set.is_empty());
            result.ssts[task.upper_level - 1] = new_upper_level_ssts;
        }

        files_to_remove.extend_from_slice(&task.upper_level_ids);
        files_to_remove.extend_from_slice(&task.lower_level_ids);

        let mut new_lower_level_ssts = result.ssts[task.lower_level]
            .iter()
            .filter_map(|x| {
                if lower_level_ids_set.remove(x) {
                    return None;
                }
                Some(*x)
            })
            .collect::<Vec<_>>();
        assert!(lower_level_ids_set.is_empty());
        new_lower_level_ssts.extend_from_slice(&task.lower_level_ids);

        result.ssts[task.lower_level] = new_lower_level_ssts;

        (result, files_to_remove)
    }
}

fn find_merge_overlapping_ssts(
    core: &Core,
    wait_merge_ssts: &[u32],
    target_level: usize,
) -> Vec<u32> {
    let first_key = wait_merge_ssts
        .iter()
        .map(|id| core.ssts_map[id].first_key())
        .min()
        .unwrap();
    let last_key = wait_merge_ssts
        .iter()
        .map(|id| core.ssts_map[id].last_key())
        .max()
        .unwrap();

    let mut overlap_ssts = Vec::new();
    for sst_id in core.ssts[target_level].iter() {
        let tb = &core.ssts_map[sst_id];
        if !(tb.last_key < first_key || tb.first_key > last_key) {
            overlap_ssts.push(*sst_id);
        }
    }

    tracing::debug!(
        "find merge overlapping ssts: {:?}, await to merge ssts: {:?}, target_level: {:?}",
        overlap_ssts,
        wait_merge_ssts,
        target_level
    );

    overlap_ssts
}
