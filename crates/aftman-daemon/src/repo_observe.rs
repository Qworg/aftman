//! In-process repository observation with `gix`.
//!
//! The observation must never execute a program the repository names. `git` cannot promise
//! that: a clean/smudge/process filter, `core.fsmonitor`, a hook, `diff.external` or a
//! `diff.<driver>.textconv` command are all programs git will run, and git has no switch that
//! disables the filter family. The previous design forced known-dangerous settings off and
//! overrode every `filter.<name>` driver it could see, but the worker runs concurrently and
//! can insert a driver between that scan and the `git diff` that follows.
//!
//! This module removes the window **by construction**. The only object that can name a
//! program is a filter driver, and every conversion goes through a
//! [`gix::filter::plumbing::Pipeline`] this code builds with an **empty driver list**.
//! A `.gitattributes` line selecting `filter=anything` therefore matches nothing, because
//! the driver vector it is looked up in is empty — it is not a list of names to deny, it is
//! a list of programs we are willing to run, and it is empty. The same is true of the
//! diff-driver list on the [`gix::diff::blob::Platform`] (textconv and binary-to-text), and
//! we never call `prepare_diff_command` or `prepare_binary_to_text_cmd`. `Mode::ToGit` makes
//! `to_worktree()` false, so binary-to-text is unreachable even if a driver existed. Hooks
//! and `core.fsmonitor` are simply not implemented by the crates used here.
//!
//! Everything runs on a blocking thread ([`crate::obs`] wraps it in `spawn_blocking`), under
//! one deadline and an [`AtomicBool`] interrupt that the status walk polls. Files are read
//! with `O_NONBLOCK | O_NOFOLLOW` so a FIFO, device or symlink planted at a tracked or
//! untracked path cannot block or redirect the read.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt as _;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use gix::bstr::{BStr, ByteSlice};
use gix::diff::blob::pipeline::{Mode, WorktreeRoots};
use gix::diff::blob::platform::prepare_diff::Operation;
use gix::diff::blob::platform::resource::Data;
use gix::diff::blob::unified_diff::{ConsumeHunk, ContextSize, DiffLineKind, HunkHeader};
use gix::diff::blob::{Diff, InternedInput, ResourceKind, UnifiedDiff};
use gix::objs::tree::EntryKind;
use gix::status::plumbing::index_as_worktree::traits::{CompareBlobs, ReadData, SubmoduleStatus};
use gix::status::plumbing::index_as_worktree::{Change, EntryStatus};
use gix::status::plumbing::index_as_worktree_with_renames::{
    self as iw, Options as IwOptions, Sorting, VisitEntry,
};
use gix::worktree::IndexPersistedOrInMemory;

use aftman_core::observation::{GitEvidence, RepoInstructions, UntrackedExcerpt, cap_text};

use crate::obs::{MAX_GIT_OUTPUT_BYTES, ObsCaps};

/// A worktree file larger than this is diffed as if it were binary, so a single huge file
/// cannot make the observation read unbounded bytes into memory. `git` would diff it as text;
/// the size cap is a deliberate, documented deviation.
const MAX_DIFF_BLOB_BYTES: u64 = 8 * 1024 * 1024;

/// A worktree file larger than this is reported as changed without hashing it. `git status`
/// hashes any file whose stat is not a match; on a huge file that is the one part of the walk
/// that is not interruptible per entry. Reporting it as changed is conservative.
const MAX_HASH_BYTES: u64 = 32 * 1024 * 1024;

/// Everything one observation of the repository produced.
pub(crate) struct Observed {
    pub git: GitEvidence,
    pub untracked: Vec<UntrackedExcerpt>,
    pub instructions: Option<RepoInstructions>,
    /// Set when the repository could not be read at all; `git` and `untracked` are empty then,
    /// and the caller records this as `observation_note`.
    pub note: Option<String>,
}

/// Observe the repository. Runs on a blocking thread; the caller owns the deadline and sets
/// `interrupt` when it passes.
pub(crate) fn observe(repo: &Path, caps: ObsCaps, interrupt: &Arc<AtomicBool>) -> Observed {
    // Instructions are a plain bounded file read and are available even when the repository
    // itself cannot be opened, matching the CLI path that read them unconditionally.
    let instructions = read_instructions(repo, caps.instruction_chars);
    match git_evidence(repo, caps, interrupt) {
        Ok((git, untracked)) => Observed {
            git,
            untracked,
            instructions,
            note: None,
        },
        Err(note) => Observed {
            git: GitEvidence::default(),
            untracked: Vec::new(),
            instructions,
            note: Some(note),
        },
    }
}

/// One tracked entry from the index-vs-worktree walk, detached from the index it borrows.
struct Tracked {
    path: Vec<u8>,
    /// The `Y` column of `git status --short`: worktree vs index.
    y: u8,
    old_id: gix::hash::ObjectId,
    old_mode: Option<EntryKind>,
    /// Whether this entry has content (or mode) worth diffing.
    diffable: bool,
}

/// The most untracked paths collected before the excerpt budget even applies.
///
/// A worker can generate build output faster than the supervisor can describe it, and every
/// untracked path here reaches a string the model is sent.
const MAX_UNTRACKED_PATHS: usize = 4_000;

/// The most status lines rendered into `status_short`.
///
/// Git prints one line per changed path, and the repository is worker-controlled: an
/// unbounded status string breaks hard rule 5 and, worse, can push the request past the
/// model budget, which is then reported as a *model* failure rather than as a large
/// repository.
const MAX_STATUS_ENTRIES: usize = 2_000;

/// The most bytes of status text. A count alone is not a bound, because paths can be long.
const MAX_STATUS_BYTES: usize = 256 * 1024;

#[derive(Default)]
struct WalkCollector {
    tracked: Vec<Tracked>,
    untracked: Vec<Vec<u8>>,
    /// Set when a bound above stopped the walk from recording everything it saw.
    capped: bool,
}

impl WalkCollector {
    fn push_untracked(&mut self, path: Vec<u8>) {
        if self.untracked.len() >= MAX_UNTRACKED_PATHS {
            self.capped = true;
        } else {
            self.untracked.push(path);
        }
    }
}

impl<'index> VisitEntry<'index> for WalkCollector {
    type ContentChange = gix::hash::ObjectId;
    type SubmoduleStatus = ();

    fn visit_entry(
        &mut self,
        entry: iw::Entry<'index, Self::ContentChange, Self::SubmoduleStatus>,
    ) {
        match entry {
            iw::Entry::Modification {
                entry: index_entry,
                rela_path: path,
                status,
                ..
            } => {
                let (y, diffable) = match status {
                    EntryStatus::Change(Change::Modification { .. }) => (b'M', true),
                    EntryStatus::Change(Change::Removed) => (b'D', true),
                    EntryStatus::Change(Change::Type { .. }) => (b'T', true),
                    EntryStatus::Change(Change::SubmoduleModification(())) => (b'M', false),
                    EntryStatus::Conflict { .. } => (b'U', false),
                    EntryStatus::IntentToAdd => (b'A', false),
                    EntryStatus::NeedsUpdate(_) => return,
                };
                self.tracked.push(Tracked {
                    path: path.as_bytes().to_vec(),
                    y,
                    old_id: index_entry.id,
                    old_mode: index_entry.mode.to_tree_entry_mode().map(|m| m.kind()),
                    diffable,
                });
            }
            iw::Entry::DirectoryContents { entry, .. } => {
                if entry.status == gix::dir::entry::Status::Untracked {
                    self.push_untracked(entry.rela_path.as_bytes().to_vec());
                }
            }
            // Rewrite tracking is disabled, so this is unreachable in practice. Record the
            // destination so the path is not silently dropped.
            iw::Entry::Rewrite { dirwalk_entry, .. } => {
                self.push_untracked(dirwalk_entry.rela_path.as_bytes().to_vec());
            }
        }
    }
}

#[derive(Clone)]
struct NoSubmodules;

impl SubmoduleStatus for NoSubmodules {
    type Output = ();

    fn status(
        &mut self,
        _entry: &gix::index::Entry,
        _relative_path: &BStr,
    ) -> gix::Result<Option<()>> {
        // `--ignore-submodules=all`: a submodule is never reported as changed.
        Ok(None)
    }
}

/// Compares two blobs by hashing, refusing to hash an oversized worktree file.
///
/// The built-in `HashEq` streams the whole file through a hash with a throwaway interrupt
/// flag, so one very large file can outlive the observation deadline. This caps the bytes we
/// are willing to read and consults the observation's interrupt flag before each file.
#[derive(Clone)]
struct BoundedHashEq {
    interrupt: Arc<AtomicBool>,
    max_bytes: u64,
}

impl CompareBlobs for BoundedHashEq {
    type Output = gix::hash::ObjectId;

    fn compare_blobs<'a, 'b>(
        &mut self,
        entry: &gix::index::Entry,
        worktree_blob_size: u64,
        data: impl ReadData<'a>,
        buf: &mut Vec<u8>,
    ) -> gix::Result<Option<Self::Output>> {
        // Returning the index id marks the entry as changed without doing the work; the
        // caller only checks `is_some()`.
        if self.interrupt.load(Ordering::Relaxed) || worktree_blob_size > self.max_bytes {
            return Ok(Some(entry.id));
        }
        // A size mismatch is conclusive without reading the file.
        if u64::from(entry.stat.size) != worktree_blob_size
            && (entry.id.is_empty_blob() || entry.stat.size != 0)
        {
            return Ok(Some(entry.id));
        }
        let mut stream = data.stream_worktree_file()?;
        let hash = match stream.as_bytes() {
            Some(bytes) => gix::objs::compute_hash(entry.id.kind(), gix::objs::Kind::Blob, bytes)?,
            None => {
                buf.clear();
                std::io::Read::read_to_end(&mut stream, buf).map_err(gix::hash::io::from_std_io)?;
                gix::objs::compute_hash(entry.id.kind(), gix::objs::Kind::Blob, buf)?
            }
        };
        Ok((entry.id != hash).then_some(hash))
    }
}

/// Build a diff/filter platform whose driver lists are empty.
///
/// Attributes and ignores are read **from the index**, not the worktree, and
/// `core.attributesFile` / `core.excludesFile` are neutralised when the repository is opened
/// ([`open_repository`]). gix opens those files with a plain `File::open`, so a worker could plant
/// a FIFO at `.gitattributes` or a `.gitignore` path and block the observation thread forever —
/// and a blocking task cannot be killed. Committed attributes are also the right authority: a
/// worker's uncommitted `.gitattributes` should not change how the supervisor reads its own
/// repository.
fn build_platform(
    repo: &gix::Repository,
    index: &gix::index::State,
    roots: &WorktreeRoots,
) -> Result<gix::diff::blob::Platform, String> {
    let stack = repo
        .attributes(
            index,
            gix::worktree::stack::state::attributes::Source::IdMapping,
            gix::worktree::stack::state::ignore::Source::IdMapping,
            None,
        )
        .map_err(|err| format!("could not build the attribute stack: {err}"))?
        .detach();
    let mut filter_options = gix::filter::Pipeline::options(repo)
        .map_err(|err| format!("could not read filter settings: {err}"))?;
    filter_options.drivers.clear();
    let empty_filter = gix::filter::plumbing::Pipeline::new(
        repo.command_context()
            .map_err(|err| format!("could not build the filter context: {err}"))?,
        repo.object_hash(),
        filter_options,
    );
    Ok(gix::diff::blob::Platform::new(
        gix::diff::blob::platform::Options {
            algorithm: None,
            skip_internal_diff_if_external_is_configured: false,
        },
        gix::diff::blob::Pipeline::new(
            roots.clone(),
            empty_filter,
            Vec::new(),
            gix::diff::blob::pipeline::Options {
                large_file_threshold_bytes: MAX_DIFF_BLOB_BYTES,
                fs: repo
                    .filesystem_options()
                    .map_err(|err| format!("could not read filesystem options: {err}"))?,
            },
        ),
        Mode::ToGit,
        stack,
    ))
}

/// Refuse a non-regular file at a path gix will open.
///
/// An in-process read cannot be killed, so a FIFO planted at one of these paths blocks the
/// observation thread for good — the async deadline still fires, but the thread never comes
/// back. Git never writes any of these as anything but a regular file, so refusing is a safe
/// failure with a note rather than a hang. A missing file is fine and means nothing to check.
fn refuse_non_regular(path: &Path, what: &str) -> Result<(), String> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => Err(format!(
            "the repository's {what} is not a regular file; refusing to read it"
        )),
        _ => Ok(()),
    }
}

/// The most entries [`refuse_non_regular_tree`] will look at before giving up. A refs tree
/// with more entries than this is not something to walk, and stopping early is safe: this is
/// defence in depth behind the observation slot bound, not the thing that makes the
/// observation terminate.
const MAX_TREE_ENTRIES: usize = 100_000;

/// Refuse a non-regular entry anywhere under `dir`, iteratively so a deeply nested tree
/// cannot overflow the stack.
fn refuse_non_regular_tree(dir: &Path, what: &str) -> Result<(), String> {
    let mut stack = vec![dir.to_path_buf()];
    let mut seen = 0usize;
    while let Some(current) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            seen += 1;
            if seen > MAX_TREE_ENTRIES {
                return Ok(());
            }
            let path = entry.path();
            let Ok(metadata) = std::fs::symlink_metadata(&path) else {
                continue;
            };
            let kind = metadata.file_type();
            if kind.is_dir() {
                stack.push(path);
            } else if kind.is_symlink() || !kind.is_file() {
                return Err(format!(
                    "the repository's {what} contains an entry that is not a regular file; \
                     refusing to read it"
                ));
            }
        }
    }
    Ok(())
}

/// Open the repository read-only, refusing wholesale to let the configuration point the
/// attribute or exclude machinery at a file the worker controls outside the worktree.
fn open_repository(path: &Path) -> Result<gix::Repository, String> {
    let options = gix::open::Options::default().config_overrides([
        "core.attributesFile=/dev/null",
        "core.excludesFile=/dev/null",
    ]);
    let repo = gix::discover_opts(path, Default::default(), options)
        .map_err(|err| format!("could not open the repository read-only: {err}"))?;
    // The worktree attributes/excludes come from the index, but several files under `.git`
    // are still read from disk by gix, and every one of them is worker-controlled. A FIFO at
    // any of them blocks the observation thread for good: an in-process open cannot be
    // interrupted, so the deadline fires and the thread stays blocked. Refusing a non-regular
    // file is a safe failure with a note, not a hang.
    //
    // This is defence in depth, not the guarantee. `.git/config` is read *during* discovery,
    // before this point, and a loose object is opened wherever the diff needs it — neither can
    // be checked here. What bounds those is the observation slot in `crate::obs`: a blocked
    // thread keeps its slot, so the damage is a fixed number of threads rather than the whole
    // blocking pool. See `docs/08-security.md` §3.2.
    for name in ["info/attributes", "info/exclude"] {
        refuse_non_regular(&repo.common_dir().join(name), &format!(".git/{name}"))?;
    }
    refuse_non_regular(&repo.index_path(), ".git/index")?;
    refuse_non_regular(&repo.git_dir().join("HEAD"), ".git/HEAD")?;
    refuse_non_regular(&repo.common_dir().join("packed-refs"), ".git/packed-refs")?;
    refuse_non_regular(&repo.common_dir().join("shallow"), ".git/shallow")?;
    refuse_non_regular(
        &repo.common_dir().join("objects/info/alternates"),
        ".git/objects/info/alternates",
    )?;
    refuse_non_regular_tree(&repo.common_dir().join("refs"), ".git/refs")?;
    Ok(repo)
}

/// The `X` column of `git status --short`: HEAD vs index, with rename tracking disabled so no
/// diff resource cache (and therefore no config-derived filter pipeline) is constructed.
fn staged_columns(
    repo: &gix::Repository,
    index: &gix::index::State,
    head_tree: &gix::hash::oid,
) -> Result<BTreeMap<Vec<u8>, u8>, String> {
    let mut out: BTreeMap<Vec<u8>, u8> = BTreeMap::new();
    repo.tree_index_status(
        head_tree,
        index,
        None,
        gix::status::tree_index::TrackRenames::Disabled,
        |change, _tree_index, _index| {
            use gix::diff::index::ChangeRef;
            match change {
                ChangeRef::Addition { location, .. } => {
                    out.insert(location.as_bytes().to_vec(), b'A');
                }
                ChangeRef::Deletion { location, .. } => {
                    out.insert(location.as_bytes().to_vec(), b'D');
                }
                ChangeRef::Modification {
                    location,
                    previous_entry_mode,
                    entry_mode,
                    ..
                } => {
                    let previous = previous_entry_mode.to_tree_entry_mode().map(|m| m.kind());
                    let new = entry_mode.to_tree_entry_mode().map(|m| m.kind());
                    let letter = if previous != new { b'T' } else { b'M' };
                    out.insert(location.as_bytes().to_vec(), letter);
                }
                ChangeRef::Rewrite { location, .. } => {
                    out.insert(location.as_bytes().to_vec(), b'R');
                }
            }
            Ok(std::ops::ControlFlow::Continue(()))
        },
    )
    .map_err(|err| format!("could not compare the index to HEAD: {err}"))?;
    Ok(out)
}

/// Build the status text from the two status walks: `HEAD`-vs-index in column one,
/// index-vs-worktree in column two, with untracked entries after every tracked one, as git
/// prints them.
fn status_short(
    staged: &BTreeMap<Vec<u8>, u8>,
    tracked: &[Tracked],
    untracked: &[Vec<u8>],
) -> (String, bool) {
    let mut rows: BTreeMap<&[u8], (u8, u8)> = BTreeMap::new();
    for (path, x) in staged {
        rows.insert(path.as_slice(), (*x, b' '));
    }
    for entry in tracked {
        let row = rows.entry(entry.path.as_slice()).or_insert((b' ', b' '));
        row.1 = entry.y;
    }
    let mut untracked_sorted: Vec<&[u8]> = untracked.iter().map(Vec::as_slice).collect();
    untracked_sorted.sort_unstable();
    untracked_sorted.dedup();

    // Both bounds are checked per line: a count stops a million tiny paths, and a byte
    // budget stops a thousand enormous ones.
    let mut out = String::new();
    let mut entries = 0usize;
    let mut capped = false;
    for (path, (x, y)) in rows {
        if entries >= MAX_STATUS_ENTRIES || out.len() >= MAX_STATUS_BYTES {
            capped = true;
            break;
        }
        out.push_str(&format!(
            "{}{} {}\n",
            x as char,
            y as char,
            quote_path(path, true)
        ));
        entries += 1;
    }
    if !capped {
        for path in untracked_sorted {
            if entries >= MAX_STATUS_ENTRIES || out.len() >= MAX_STATUS_BYTES {
                capped = true;
                break;
            }
            out.push_str(&format!("?? {}\n", quote_path(path, true)));
            entries += 1;
        }
    }
    (out, capped)
}

fn git_evidence(
    repo_path: &Path,
    caps: ObsCaps,
    interrupt: &Arc<AtomicBool>,
) -> Result<(GitEvidence, Vec<UntrackedExcerpt>), String> {
    let repo = open_repository(repo_path)?;
    let workdir = repo
        .workdir()
        .ok_or_else(|| "the repository has no working tree".to_owned())?
        .to_owned();

    // `discover_opts` searches upward. Without this, a path that is not itself a
    // working-tree root would silently describe whatever repository encloses it — reporting
    // changes outside the job, with no note, so `UnobservedRepository` would not gate
    // `Finish` either. The job names the repository it wants observed.
    let requested = repo_path
        .canonicalize()
        .map_err(|err| format!("could not resolve the repository path: {err}"))?;
    let root = workdir
        .canonicalize()
        .map_err(|err| format!("could not resolve the working tree: {err}"))?;
    if root != requested {
        return Err(format!(
            "{} is not the root of a git working tree (its repository root is {}); \
             aftman observes the repository a job names, not one enclosing it",
            requested.display(),
            root.display()
        ));
    }

    let index_holder = repo
        .index_or_load_from_head_or_empty()
        .map_err(|err| format!("could not read the index: {err}"))?;
    let index: &gix::index::State = match &index_holder {
        IndexPersistedOrInMemory::Persisted(index) => index,
        IndexPersistedOrInMemory::InMemory(index) => index,
    };

    let head_tree = repo
        .head_tree_id_or_empty()
        .map_err(|err| format!("could not resolve HEAD: {err}"))?;
    let staged = staged_columns(&repo, index, &head_tree)?;

    let roots = WorktreeRoots {
        old_root: None,
        new_root: Some(workdir.clone()),
    };
    // One empty-driver platform drives the worktree-vs-index walk, a second the diff
    // rendering. Neither can name a program.
    let status_platform = build_platform(&repo, index, &roots)?;
    let diff_platform = build_platform(&repo, index, &roots)?;

    let objects =
        repo.objects.clone().into_arc().map_err(|err| {
            format!("the object database could not be shared across threads: {err}")
        })?;

    let dirwalk_options: gix::dir::walk::Options = repo
        .dirwalk_options()
        .map_err(|err| format!("could not read directory-walk options: {err}"))?
        .into();
    let pathspec = repo
        .pathspec(
            false,
            None::<&str>,
            false,
            index,
            gix::worktree::stack::state::attributes::Source::WorktreeThenIdMapping,
        )
        .map_err(|err| format!("could not build a pathspec: {err}"))?
        .into_parts()
        .0;

    let git_dir_realpath = gix::path::realpath_opts(
        repo.git_dir(),
        repo.current_dir(),
        gix::path::realpath::MAX_SYMLINKS,
    )
    .map_err(|err| format!("could not resolve the git directory: {err}"))?;
    let fs_caps = repo
        .filesystem_options()
        .map_err(|err| format!("could not read filesystem options: {err}"))?;
    let accelerate = fs_caps.ignore_case.then(|| index.prepare_icase_backing());

    let mut collector = WalkCollector::default();
    let mut progress = gix::progress::Discard;
    gix::status::plumbing::index_as_worktree_with_renames(
        index,
        &workdir,
        &mut collector,
        BoundedHashEq {
            interrupt: interrupt.clone(),
            max_bytes: MAX_HASH_BYTES,
        },
        NoSubmodules,
        objects.clone(),
        &mut progress,
        iw::Context {
            pathspec,
            resource_cache: status_platform,
            should_interrupt: interrupt,
            dirwalk: iw::DirwalkContext {
                git_dir_realpath: git_dir_realpath.as_path(),
                current_dir: repo.current_dir(),
                ignore_case_index_lookup: accelerate.as_ref(),
            },
        },
        IwOptions {
            sorting: Some(Sorting::ByPathCaseSensitive),
            tracked_file_modifications: gix::status::plumbing::index_as_worktree::Options {
                fs: fs_caps,
                thread_limit: None,
                stat: repo
                    .stat_options()
                    .map_err(|err| format!("could not read stat options: {err}"))?,
                fscache: false,
            },
            fscache: false,
            dirwalk: Some(dirwalk_options),
            rewrites: None,
        },
    )
    .map_err(|err| format!("could not walk the worktree status: {err}"))?;

    let changed_files: Vec<String> = {
        let mut all: Vec<&[u8]> = collector
            .tracked
            .iter()
            .map(|t| t.path.as_slice())
            .chain(collector.untracked.iter().map(Vec::as_slice))
            .collect();
        all.sort_unstable();
        all.dedup();
        all.into_iter()
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect()
    };

    let (full_diff, diff_stat) = diff_and_stat(
        &repo,
        diff_platform,
        &objects,
        &collector,
        &workdir,
        caps,
        interrupt,
    )?;

    let (diff_excerpt, diff_capped) = cap_text(&full_diff, caps.diff_chars);
    let untracked = untracked_excerpts(&collector.untracked, &workdir, caps);

    let head_sha = repo
        .head_id()
        .ok()
        .map(|id| id.detach().to_string())
        .filter(|s| !s.is_empty());
    let branch = repo
        .head_name()
        .ok()
        .flatten()
        .map(|name| name.shorten().to_string());

    let (status_short, status_capped) =
        status_short(&staged, &collector.tracked, &collector.untracked);
    // `truncated` means "what you are being shown is not all of it", so every bound counts:
    // the diff excerpt, the status text, and the untracked list.
    let truncated = diff_capped || status_capped || collector.capped;

    Ok((
        GitEvidence {
            status_short,
            diff_excerpt,
            diff_stat,
            changed_files,
            head_sha,
            branch,
            truncated,
        },
        untracked,
    ))
}

/// One line of `git diff --stat`.
struct StatLine {
    path: Vec<u8>,
    insertions: u32,
    removals: u32,
    /// `(old, new)` sizes when the file is treated as binary.
    binary: Option<(u64, u64)>,
    /// A mode-only change, which git shows as `0`.
    mode_only: bool,
}

#[allow(clippy::too_many_arguments)]
fn diff_and_stat(
    repo: &gix::Repository,
    mut platform: gix::diff::blob::Platform,
    objects: &gix::OdbHandleArc,
    collector: &WalkCollector,
    workdir: &Path,
    caps: ObsCaps,
    interrupt: &AtomicBool,
) -> Result<(String, String), String> {
    let null = repo.object_hash().null();
    let mut diffs = String::new();
    // Keep rendering a little past the excerpt cap so `cap_text` reports truncation
    // accurately without walking an unbounded number of files.
    let diff_budget = caps.diff_chars.saturating_mul(4).saturating_add(4096);
    let mut stats: Vec<StatLine> = Vec::new();

    for entry in &collector.tracked {
        if interrupt.load(Ordering::Relaxed) || diffs.len() >= diff_budget {
            break;
        }
        // A conflicted, intent-to-add or submodule entry has no blob pair to diff — but it
        // must not *stop* the walk. `collector.tracked` is sorted by path, so `break` here
        // silently dropped the diff and stat of every path that sorts after the first such
        // entry, leaving the model "modified, but nothing to show" with `truncated` false.
        if !entry.diffable {
            continue;
        }
        let relative = entry.path.as_bstr();
        let old_kind = entry.old_mode.unwrap_or(EntryKind::Blob);
        let worktree_path = workdir.join(OsStr::from_bytes(&entry.path));
        let new_kind = worktree_kind(&worktree_path);

        platform.clear_resource_cache_keep_allocation();
        if platform
            .set_resource(
                entry.old_id,
                old_kind,
                relative,
                ResourceKind::OldOrSource,
                objects,
            )
            .is_err()
        {
            // The old blob is missing (a partial clone). The path is still listed in the
            // status; its diff and stat are omitted rather than failing the whole cycle.
            continue;
        }
        // Nothing to read on the new side: with no worktree root a null id is "missing"
        // without touching the path, so a directory, FIFO or device is never opened.
        let saved_root = platform.filter.roots.new_root.take();
        let new_set = match new_kind {
            Some(kind) => {
                platform.filter.roots.new_root = saved_root;
                platform.set_resource(
                    null,
                    kind,
                    relative,
                    ResourceKind::NewOrDestination,
                    objects,
                )
            }
            None => {
                platform.filter.roots.new_root = None;
                let result = platform.set_resource(
                    null,
                    old_kind,
                    relative,
                    ResourceKind::NewOrDestination,
                    objects,
                );
                platform.filter.roots.new_root = saved_root;
                result
            }
        };
        if new_set.is_err() {
            continue;
        }

        let prep = match platform.prepare_diff() {
            Ok(prep) => prep,
            Err(_) => continue,
        };
        match prep.operation {
            Operation::InternalDiff { algorithm } => {
                let input = prep.interned_input();
                let diff = Diff::compute(algorithm, &input);
                let insertions = diff.count_additions();
                let removals = diff.count_removals();
                let hunks = render_hunks(&diff, &input);
                let new_id = match prep.new.data {
                    Data::Buffer { buf, .. } => Some(
                        gix::objs::compute_hash(entry.old_id.kind(), gix::objs::Kind::Blob, buf)
                            .map_err(|err| format!("could not hash the worktree file: {err}"))?,
                    ),
                    _ => None,
                };
                if hunks.is_empty() && new_kind == Some(old_kind) {
                    // Stat matched but the content did not change after all; nothing to show.
                    continue;
                }
                diffs.push_str(&render_text_file(
                    &entry.path,
                    entry.old_id,
                    new_id,
                    old_kind,
                    new_kind,
                    &hunks,
                ));
                stats.push(StatLine {
                    path: entry.path.clone(),
                    insertions,
                    removals,
                    binary: None,
                    mode_only: hunks.is_empty(),
                });
            }
            Operation::SourceOrDestinationIsBinary => {
                let old_size = data_len(&prep.old.data);
                let new_size = data_len(&prep.new.data);
                diffs.push_str(&render_binary_file(&entry.path));
                stats.push(StatLine {
                    path: entry.path.clone(),
                    insertions: 0,
                    removals: 0,
                    binary: Some((old_size, new_size)),
                    mode_only: false,
                });
            }
            // `skip_internal_diff_if_external_is_configured` is false and we call no
            // external-diff API, so this cannot occur. Ignoring it cannot run anything.
            Operation::ExternalCommand { .. } => continue,
        }
    }

    let mut diff_stat = render_stat(&stats);
    if diff_stat.len() > MAX_GIT_OUTPUT_BYTES {
        diff_stat.truncate(MAX_GIT_OUTPUT_BYTES);
        diff_stat.push_str("\n[stat truncated]\n");
    }
    Ok((diffs, diff_stat))
}

fn render_text_file(
    path: &[u8],
    old_id: gix::hash::ObjectId,
    new_id: Option<gix::hash::ObjectId>,
    old_kind: EntryKind,
    new_kind: Option<EntryKind>,
    hunks: &str,
) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "diff --git {} {}\n",
        diff_path(b"a/", path),
        diff_path(b"b/", path)
    ));
    match new_kind {
        None => {
            out.push_str(&format!("deleted file mode {}\n", old_kind.as_octal_str()));
            out.push_str(&format!("index {}..0000000\n", short_id(old_id)));
            out.push_str(&format!("--- {}\n", diff_marker(b"a/", path)));
            out.push_str("+++ /dev/null\n");
        }
        Some(kind) => {
            let mode_changed = kind != old_kind;
            if mode_changed {
                out.push_str(&format!("old mode {}\n", old_kind.as_octal_str()));
                out.push_str(&format!("new mode {}\n", kind.as_octal_str()));
            }
            if let Some(new_id) = new_id {
                if mode_changed {
                    out.push_str(&format!(
                        "index {}..{}\n",
                        short_id(old_id),
                        short_id(new_id)
                    ));
                } else {
                    out.push_str(&format!(
                        "index {}..{} {}\n",
                        short_id(old_id),
                        short_id(new_id),
                        kind.as_octal_str()
                    ));
                }
            }
            out.push_str(&format!("--- {}\n", diff_marker(b"a/", path)));
            out.push_str(&format!("+++ {}\n", diff_marker(b"b/", path)));
        }
    }
    out.push_str(hunks);
    out
}

/// `git`'s binary-file diff. The `index` line is omitted because the converted worktree
/// content is not retained once gix has classified it as binary; the size pair is what
/// `--stat` needs. Named deviation.
fn render_binary_file(path: &[u8]) -> String {
    format!(
        "diff --git {a} {b}\nBinary files {a} and {b} differ\n",
        a = diff_path(b"a/", path),
        b = diff_path(b"b/", path)
    )
}

fn worktree_kind(path: &Path) -> Option<EntryKind> {
    let metadata = std::fs::symlink_metadata(path).ok()?;
    if metadata.file_type().is_symlink() {
        Some(EntryKind::Link)
    } else if metadata.is_file() {
        Some(if gix::fs::is_executable(&metadata) {
            EntryKind::BlobExecutable
        } else {
            EntryKind::Blob
        })
    } else {
        None
    }
}

fn data_len(data: &Data<'_>) -> u64 {
    match data {
        Data::Missing => 0,
        Data::Buffer { buf, .. } => u64::try_from(buf.len()).unwrap_or(u64::MAX),
        Data::Binary { size } => *size,
    }
}

fn short_id(id: gix::hash::ObjectId) -> String {
    let hex = id.to_string();
    hex[..hex.len().min(7)].to_owned()
}

fn diff_path(prefix: &[u8], path: &[u8]) -> String {
    let mut joined = Vec::with_capacity(prefix.len() + path.len());
    joined.extend_from_slice(prefix);
    joined.extend_from_slice(path);
    quote_path(&joined, false)
}

/// `git` appends a tab to a `---`/`+++` line whose path contains a space and is not quoted,
/// so the space cannot be confused with the separating tab.
fn diff_marker(prefix: &[u8], path: &[u8]) -> String {
    let quoted = diff_path(prefix, path);
    if path.contains(&b' ') && !quoted.starts_with('"') {
        format!("{quoted}\t")
    } else {
        quoted
    }
}

fn render_hunks(diff: &Diff, input: &InternedInput<&[u8]>) -> String {
    let delegate = GitHunks(String::new());
    UnifiedDiff::new(diff, input, delegate, ContextSize::symmetrical(3))
        .consume()
        .unwrap_or_default()
}

struct GitHunks(String);

impl ConsumeHunk for GitHunks {
    type Out = String;

    fn consume_hunk(
        &mut self,
        header: HunkHeader,
        lines: &[(DiffLineKind, &[u8])],
    ) -> std::io::Result<()> {
        self.0.push_str(&hunk_header(header));
        self.0.push('\n');
        for (kind, content) in lines {
            self.0.push(kind.to_prefix());
            self.0.push_str(&String::from_utf8_lossy(content));
            self.0.push('\n');
        }
        Ok(())
    }

    fn finish(self) -> String {
        self.0
    }
}

/// git omits `,len` when a range covers one line, and prints the line *before* an empty range.
fn hunk_header(header: HunkHeader) -> String {
    fn range(start: u32, len: u32) -> String {
        match len {
            0 => format!("{},0", start.saturating_sub(1)),
            1 => start.to_string(),
            _ => format!("{start},{len}"),
        }
    }
    format!(
        "@@ -{} +{} @@",
        range(header.before_hunk_start, header.before_hunk_len),
        range(header.after_hunk_start, header.after_hunk_len)
    )
}

fn render_stat(stats: &[StatLine]) -> String {
    if stats.is_empty() {
        return String::new();
    }
    let name_width = stats
        .iter()
        .map(|s| quote_path(&s.path, false).chars().count())
        .max()
        .unwrap_or(0);
    let count_width = stats
        .iter()
        .map(|s| {
            if s.binary.is_some() {
                3 // "Bin"
            } else {
                decimal_width(s.insertions.saturating_add(s.removals))
            }
        })
        .max()
        .unwrap_or(0);
    let graph_width = 80usize.saturating_sub(name_width + count_width + 3).max(1);
    let max_change = stats
        .iter()
        .filter(|s| s.binary.is_none())
        .map(|s| s.insertions.saturating_add(s.removals))
        .max()
        .unwrap_or(0);
    let scale = if max_change as usize > graph_width {
        (max_change as usize).div_ceil(graph_width)
    } else {
        1
    };

    let mut total_insertions = 0u64;
    let mut total_removals = 0u64;
    let mut out = String::new();
    for line in stats {
        let path = quote_path(&line.path, false);
        if let Some((old, new)) = line.binary {
            out.push_str(&format!(
                " {path:<name_width$} | Bin {old} -> {new} bytes\n"
            ));
            continue;
        }
        let count = line.insertions.saturating_add(line.removals);
        total_insertions += u64::from(line.insertions);
        total_removals += u64::from(line.removals);
        let plus = (line.insertions as usize / scale).max(usize::from(line.insertions > 0));
        let minus = (line.removals as usize / scale).max(usize::from(line.removals > 0));
        let graph: String = "+".repeat(plus) + &"-".repeat(minus);
        if line.mode_only && count == 0 {
            out.push_str(&format!(" {path:<name_width$} | {count:>count_width$}\n"));
        } else {
            out.push_str(&format!(
                " {path:<name_width$} | {count:>count_width$} {graph}\n"
            ));
        }
    }

    let files = stats.len();
    let mut summary = format!(" {files} file{} changed", if files == 1 { "" } else { "s" });
    if total_insertions > 0 || total_removals == 0 {
        summary.push_str(&format!(
            ", {total_insertions} insertion{}(+)",
            if total_insertions == 1 { "" } else { "s" }
        ));
    }
    if total_removals > 0 || total_insertions == 0 {
        summary.push_str(&format!(
            ", {total_removals} deletion{}(-)",
            if total_removals == 1 { "" } else { "s" }
        ));
    }
    summary.push('\n');
    out.push_str(&summary);
    out
}

fn decimal_width(mut value: u32) -> usize {
    let mut width = 1;
    while value >= 10 {
        value /= 10;
        width += 1;
    }
    width
}

/// A C-style quoted path, as `git` prints it.
///
/// Quoting is what stops a repository-controlled path containing a newline from forging a
/// second status or diff line: `\n` becomes the two characters `\n`. `quote_space` selects
/// git's status convention (a plain space is quoted) over its diff-header convention (a plain
/// space is left alone; the caller appends a tab to `---`/`+++` instead).
fn quote_path(path: &[u8], quote_space: bool) -> String {
    let needs_quote =
        |b: u8| b < 0x20 || b == b'"' || b == b'\\' || b >= 0x80 || (quote_space && b == b' ');
    if !path.iter().any(|&b| needs_quote(b)) {
        return String::from_utf8_lossy(path).into_owned();
    }
    let mut out = String::with_capacity(path.len() + 2);
    out.push('"');
    for &b in path {
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            0x07 => out.push_str("\\a"),
            0x08 => out.push_str("\\b"),
            b'\t' => out.push_str("\\t"),
            b'\n' => out.push_str("\\n"),
            0x0b => out.push_str("\\v"),
            0x0c => out.push_str("\\f"),
            b'\r' => out.push_str("\\r"),
            _ if !(0x20..0x80).contains(&b) => out.push_str(&format!("\\{b:03o}")),
            _ => out.push(b as char),
        }
    }
    out.push('"');
    out
}

/// Bounded excerpts of untracked text files.
fn untracked_excerpts(
    untracked: &[Vec<u8>],
    workdir: &Path,
    caps: ObsCaps,
) -> Vec<UntrackedExcerpt> {
    let mut sorted: Vec<&[u8]> = untracked.iter().map(Vec::as_slice).collect();
    sorted.sort_unstable();
    sorted.dedup();

    let mut budget = caps.untracked_bytes;
    let mut excerpts = Vec::new();
    for relative in sorted {
        if excerpts.len() >= caps.untracked_files || budget == 0 {
            break;
        }
        let Ok(relative) = std::str::from_utf8(relative) else {
            continue;
        };
        if relative.is_empty() || !is_safe_to_read(relative) {
            continue;
        }
        // Read at most the budget, plus enough to see a NUL in the first 4 KiB.
        let Some((bytes, size)) = read_capped(&workdir.join(relative), budget.max(4096)) else {
            continue;
        };
        if bytes.iter().take(4096).any(|b| *b == 0) {
            continue;
        }
        let take = bytes.len().min(budget);
        let truncated = size > take as u64;
        let content = String::from_utf8_lossy(&bytes[..take]).into_owned();
        budget = budget.saturating_sub(take);
        excerpts.push(UntrackedExcerpt {
            path: relative.to_owned(),
            bytes: usize::try_from(size).unwrap_or(usize::MAX),
            content,
            truncated,
        });
    }
    excerpts
}

/// Names that must never be read into an observation.
fn is_safe_to_read(relative: &str) -> bool {
    let lower = relative.to_ascii_lowercase();
    let name = lower.rsplit('/').next().unwrap_or(&lower);
    const DENY_SUBSTRINGS: &[&str] = &["secret", "token", "credential"];
    const DENY_SUFFIXES: &[&str] = &[".pem", ".key", ".p12", ".keystore"];
    if lower.starts_with("..") || lower.contains("/../") {
        return false;
    }
    if name == ".env" || name.starts_with(".env.") {
        return false;
    }
    if name == "id_rsa" || name == "id_ed25519" || name == "id_ecdsa" {
        return false;
    }
    if DENY_SUBSTRINGS.iter().any(|s| name.contains(s)) {
        return false;
    }
    !DENY_SUFFIXES.iter().any(|s| name.ends_with(s))
}

/// Open `path` for reading only if it is a regular file, without ever blocking and without
/// following a symlink: `O_NONBLOCK` makes opening a FIFO return at once (it is then
/// rejected), `O_NOFOLLOW` rejects a symlink. The type is re-checked on the opened handle.
fn open_regular(path: &Path) -> Option<(std::fs::File, u64)> {
    use nix::fcntl::OFlag;
    use std::os::unix::fs::OpenOptionsExt as _;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags((OFlag::O_NONBLOCK | OFlag::O_NOFOLLOW).bits())
        .open(path)
        .ok()?;
    let meta = file.metadata().ok()?;
    meta.is_file().then_some((file, meta.len()))
}

/// Read at most `cap` bytes of a regular file. `None` for anything that is not a regular file.
/// The size reported is the file's whole size.
fn read_capped(path: &Path, cap: usize) -> Option<(Vec<u8>, u64)> {
    use std::io::Read as _;
    let (file, size) = open_regular(path)?;
    let mut bytes = Vec::new();
    file.take(cap as u64).read_to_end(&mut bytes).ok()?;
    Some((bytes, size))
}

/// Read repository-root instructions, bounded. Only a regular file is read.
fn read_instructions(repo: &Path, cap_chars: usize) -> Option<RepoInstructions> {
    for file in ["AGENTS.override.md", "AGENTS.md"] {
        let byte_cap = cap_chars.saturating_mul(4).saturating_add(4);
        let Some((bytes, size)) = read_capped(&repo.join(file), byte_cap) else {
            continue;
        };
        let text = String::from_utf8_lossy(&bytes);
        let (excerpt, truncated) = cap_text(&text, cap_chars);
        return Some(RepoInstructions {
            file: file.to_owned(),
            excerpt,
            truncated: truncated || size > bytes.len() as u64,
        });
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_STATUS_ENTRIES, StatLine, is_safe_to_read, quote_path, render_stat, status_short,
    };
    use std::collections::BTreeMap;

    #[test]
    fn a_huge_untracked_list_is_capped_and_reports_it() {
        // Before the cap this string grew with the repository, and could push the request
        // past the model budget — which the runtime reports as a *model* failure, a wrong
        // diagnosis for a large working tree.
        let untracked: Vec<Vec<u8>> = (0..(MAX_STATUS_ENTRIES + 50))
            .map(|i| format!("f{i:06}").into_bytes())
            .collect();
        let (text, capped) = status_short(&BTreeMap::new(), &[], &untracked);
        assert!(capped, "the cap must be reported, not applied silently");
        assert_eq!(text.lines().count(), MAX_STATUS_ENTRIES);
        assert!(text.lines().all(|line| line.starts_with("?? ")));
    }

    #[test]
    fn a_normal_status_is_not_capped_and_keeps_gits_shape() {
        let untracked = vec![b"a.txt".to_vec()];
        let (text, capped) = status_short(&BTreeMap::new(), &[], &untracked);
        assert!(!capped);
        assert_eq!(text, "?? a.txt\n");
    }

    #[test]
    fn a_newline_in_a_path_cannot_forge_a_line() {
        assert_eq!(quote_path(b"a\nb", true), "\"a\\nb\"");
        assert_eq!(quote_path(b"a\nb", false), "\"a\\nb\"");
    }

    #[test]
    fn spaces_follow_gits_two_conventions() {
        assert_eq!(quote_path(b"a b", true), "\"a b\"");
        assert_eq!(quote_path(b"a b", false), "a b");
    }

    #[test]
    fn non_ascii_is_octal_escaped() {
        assert_eq!(
            quote_path("uni-ä.txt".as_bytes(), false),
            "\"uni-\\303\\244.txt\""
        );
    }

    #[test]
    fn stat_matches_gits_layout_for_a_small_change() {
        let stats = vec![
            StatLine {
                path: b"a.txt".to_vec(),
                insertions: 1,
                removals: 1,
                binary: None,
                mode_only: false,
            },
            StatLine {
                path: b"b.rs".to_vec(),
                insertions: 0,
                removals: 1,
                binary: None,
                mode_only: false,
            },
        ];
        assert_eq!(
            render_stat(&stats),
            " a.txt | 2 +-\n b.rs  | 1 -\n 2 files changed, 1 insertion(+), 2 deletions(-)\n"
        );
    }

    #[test]
    fn sensitive_paths_are_never_read() {
        for path in [
            ".env",
            ".env.local",
            "config/.env",
            "id_rsa",
            "certs/server.pem",
            "deploy.key",
            "src/secrets.rs",
            "lib/token_store.rs",
            "../outside.txt",
            "a/../../outside.txt",
        ] {
            assert!(!is_safe_to_read(path), "{path} must not be read");
        }
    }

    #[test]
    fn ordinary_paths_are_readable() {
        for path in [
            "src/http.rs",
            "tests/http_retry.rs",
            "README.md",
            "docs/notes.md",
        ] {
            assert!(is_safe_to_read(path), "{path} should be readable");
        }
    }

    #[test]
    fn stat_shows_binary_sizes_and_a_mode_only_zero() {
        let stats = vec![
            StatLine {
                path: b"bin.dat".to_vec(),
                insertions: 0,
                removals: 0,
                binary: Some((9, 9)),
                mode_only: false,
            },
            StatLine {
                path: b"m".to_vec(),
                insertions: 0,
                removals: 0,
                binary: None,
                mode_only: true,
            },
        ];
        assert_eq!(
            render_stat(&stats),
            " bin.dat | Bin 9 -> 9 bytes\n m       |   0\n 2 files changed, 0 insertions(+), 0 deletions(-)\n"
        );
    }
}
