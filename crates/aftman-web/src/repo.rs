//! Which directories a run may be pointed at.
//!
//! `POST /v1/runs` takes a filesystem path, and a run's observation is readable afterwards,
//! so without an allow-list a token holder could point a run at `/etc` or `~/.ssh` and read
//! what the observation collected ([08-security.md §4](../../../docs/08-security.md)).
//!
//! Every path is canonicalised — symlinks resolved, so a link cannot walk out of a root —
//! and then required to be *under* one of the roots. The comparison is by path component,
//! never by string prefix: `/home/mew` is not under `/home/me`.

use std::io;
use std::path::{Component, Path, PathBuf};

/// Why a repository path was refused. Each variant is a distinct thing the person who typed
/// the path can act on.
#[derive(Debug, thiserror::Error)]
pub enum RepoError {
    #[error("repo must be an absolute path, and {} is not", .path.display())]
    NotAbsolute { path: PathBuf },
    #[error("repo {} does not exist", .path.display())]
    Missing { path: PathBuf },
    #[error("repo {} is not a directory", .path.display())]
    NotADirectory { path: PathBuf },
    #[error(
        "repo {} is not under any of web.allowed_repo_roots ({}): a run may only read a \
         repository an operator allowed, because its observation is readable afterwards",
        .path.display(),
        roots.join(", ")
    )]
    OutsideRoots { path: PathBuf, roots: Vec<String> },
    #[error(
        "repo {} is not a git work tree, and [web] allow_non_git_repos is false: a tree that \
         is not a repository is almost always a mistake or a probe",
        .path.display()
    )]
    NotAGitRepository { path: PathBuf },
    #[error("cannot resolve {}: {source}", .path.display())]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// Resolve and validate a repository path.
///
/// `roots` must already be absolute (the configuration refuses anything else). Returns the
/// canonical path, which is what the run is then started with, so the daemon and the page
/// agree on which directory was used.
pub fn resolve(path: &Path, roots: &[PathBuf], allow_non_git: bool) -> Result<PathBuf, RepoError> {
    if !path.is_absolute() {
        return Err(RepoError::NotAbsolute {
            path: path.to_path_buf(),
        });
    }
    let canonical = path.canonicalize().map_err(|source| match source.kind() {
        io::ErrorKind::NotFound => RepoError::Missing {
            path: path.to_path_buf(),
        },
        _ => RepoError::Io {
            path: path.to_path_buf(),
            source,
        },
    })?;
    let metadata = std::fs::metadata(&canonical).map_err(|source| RepoError::Io {
        path: canonical.clone(),
        source,
    })?;
    if !metadata.is_dir() {
        return Err(RepoError::NotADirectory { path: canonical });
    }

    // Canonicalise the roots too: `/home/me/Development` may itself be a symlink, and a
    // root that is not canonical would refuse everything under it.
    let mut inside = false;
    let mut rendered = Vec::with_capacity(roots.len());
    for root in roots {
        let canonical_root = root.canonicalize().unwrap_or_else(|_| root.clone());
        rendered.push(canonical_root.display().to_string());
        if canonical.starts_with(&canonical_root) {
            inside = true;
        }
    }
    if !inside {
        return Err(RepoError::OutsideRoots {
            path: canonical,
            roots: rendered,
        });
    }

    if !allow_non_git && !is_git_work_tree(&canonical) {
        return Err(RepoError::NotAGitRepository { path: canonical });
    }
    Ok(canonical)
}

/// Whether a directory is a git work tree.
///
/// A `.git` that is a directory is an ordinary repository; a `.git` that is a *file* holds
/// `gitdir: …` and is a linked worktree or a submodule. Both are work trees. Nothing here
/// parses the file — the question is only "does git consider this the root of a tree", and
/// the in-process observation is what reads it afterwards.
#[must_use]
pub fn is_git_work_tree(path: &Path) -> bool {
    let dot_git = path.join(".git");
    match std::fs::symlink_metadata(&dot_git) {
        Ok(metadata) => metadata.is_dir() || metadata.is_file(),
        Err(_) => false,
    }
}

/// Whether `child` is under `root`, by path component rather than by string prefix.
///
/// Exposed because the tests for [`resolve`] are clearer when this is its own claim.
#[must_use]
pub fn is_under(child: &Path, root: &Path) -> bool {
    if !child.starts_with(root) {
        return false;
    }
    // `starts_with` on paths is already component-wise, but assert the property the
    // doc comment promises: no `..` can survive canonicalisation, and a component count
    // of zero would mean the two are equal, which "under" allows.
    child
        .components()
        .zip(root.components())
        .all(|(a, b)| a == b)
        && child
            .components()
            .skip(root.components().count())
            .all(|component| !matches!(component, Component::ParentDir))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn repo(root: &Path, name: &str) -> PathBuf {
        let path = root.join(name);
        std::fs::create_dir_all(path.join(".git")).expect("mkdir");
        path
    }

    #[test]
    fn a_repository_under_an_allowed_root_is_accepted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().canonicalize().expect("canonical");
        let path = repo(&root, "project");
        let resolved = resolve(&path, std::slice::from_ref(&root), false).expect("accepted");
        assert_eq!(resolved, path.canonicalize().expect("canonical"));
    }

    // The finding: a run's observation is readable afterwards, so a path outside the roots
    // is a way to read files aftman was never meant to collect.
    #[test]
    fn a_path_outside_every_root_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("allowed");
        std::fs::create_dir_all(&root).expect("mkdir");
        let outside = repo(dir.path(), "secret");
        let error = resolve(&outside, &[root], false).expect_err("must refuse");
        assert!(
            matches!(error, RepoError::OutsideRoots { .. }),
            "got {error:?}"
        );
        assert!(error.to_string().contains("allowed_repo_roots"));
    }

    // A string-prefix check would accept `/home/mew` for the root `/home/me`.
    #[test]
    fn a_sibling_with_a_shared_prefix_is_not_under_the_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = repo(dir.path(), "me");
        let sibling = repo(dir.path(), "mew");
        let error = resolve(&sibling, std::slice::from_ref(&root), false).expect_err("must refuse");
        assert!(
            matches!(error, RepoError::OutsideRoots { .. }),
            "got {error:?}"
        );
        assert!(!is_under(
            &sibling.canonicalize().expect("canonical"),
            &root.canonicalize().expect("canonical")
        ));
    }

    // Canonicalisation is what stops a symlink from walking out of a root.
    #[test]
    fn a_symlink_out_of_a_root_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("allowed");
        std::fs::create_dir_all(&root).expect("mkdir");
        let outside = repo(dir.path(), "secret");
        let link = root.join("shortcut");
        std::os::unix::fs::symlink(&outside, &link).expect("symlink");
        let error = resolve(&link, &[root], false).expect_err("must refuse");
        assert!(
            matches!(error, RepoError::OutsideRoots { .. }),
            "got {error:?}"
        );
    }

    #[test]
    fn a_relative_path_is_refused() {
        let error = resolve(Path::new("project"), &[], false).expect_err("must refuse");
        assert!(
            matches!(error, RepoError::NotAbsolute { .. }),
            "got {error:?}"
        );
    }

    #[test]
    fn a_missing_path_is_its_own_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().canonicalize().expect("canonical");
        let error = resolve(&root.join("nope"), std::slice::from_ref(&root), false)
            .expect_err("must refuse");
        assert!(matches!(error, RepoError::Missing { .. }), "got {error:?}");
    }

    #[test]
    fn a_file_is_not_a_repository() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().canonicalize().expect("canonical");
        let file = root.join("a-file");
        std::fs::write(&file, "x").expect("write");
        let error = resolve(&file, std::slice::from_ref(&root), false).expect_err("must refuse");
        assert!(
            matches!(error, RepoError::NotADirectory { .. }),
            "got {error:?}"
        );
    }

    #[test]
    fn a_tree_that_is_not_a_repository_is_refused_unless_allowed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().canonicalize().expect("canonical");
        let plain = root.join("plain");
        std::fs::create_dir_all(&plain).expect("mkdir");
        let error = resolve(&plain, std::slice::from_ref(&root), false).expect_err("must refuse");
        assert!(
            matches!(error, RepoError::NotAGitRepository { .. }),
            "got {error:?}"
        );
        // ...and accepted when the operator says so.
        assert!(resolve(&plain, std::slice::from_ref(&root), true).is_ok());
    }

    #[test]
    fn a_linked_worktree_is_a_work_tree() {
        // A linked worktree and a submodule both have a `.git` *file*.
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().canonicalize().expect("canonical");
        let worktree = root.join("linked");
        std::fs::create_dir_all(&worktree).expect("mkdir");
        std::fs::write(
            worktree.join(".git"),
            "gitdir: /somewhere/.git/worktrees/linked\n",
        )
        .expect("write");
        assert!(is_git_work_tree(&worktree));
        assert!(resolve(&worktree, std::slice::from_ref(&root), false).is_ok());
    }

    #[test]
    fn the_resolved_path_is_the_canonical_one() {
        // So the daemon and the page agree on which directory was used, even when the
        // path the user typed had a `..` or a symlink in it.
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().canonicalize().expect("canonical");
        let path = repo(&root, "project");
        let indirect = root.join("project").join("..").join("project");
        let resolved = resolve(&indirect, std::slice::from_ref(&root), false).expect("accepted");
        assert_eq!(resolved, path.canonicalize().expect("canonical"));
        assert!(!resolved.to_string_lossy().contains(".."));
    }
}
