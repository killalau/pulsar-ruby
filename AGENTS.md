# Project Instructions

## Documentation Rules

Documentation lives under `docs/`.

Every documentation folder must contain an `overview.md` file, including
`docs/` itself and every subfolder below it.

Each `overview.md` file is the table of contents for its folder. It must link to
and briefly describe:

- Every Markdown file directly inside that folder, except itself.
- Every direct child documentation folder, by linking to that child folder's
  `overview.md`.

No Markdown document under `docs/` should be unreachable from
`docs/overview.md`. When adding, moving, renaming, or deleting a Markdown file,
update the nearest `overview.md` and any parent overview needed to preserve the
link path from `docs/overview.md`.

Prefer short descriptions in overview files. They should help readers decide
where to go next, not duplicate the target document.

## Development Rules

Use test-driven development for implementation work.

For each new behavior or bug fix:

- Write or update a focused failing test first.
- Run the targeted test and confirm it fails for the expected reason.
- Implement the smallest change that makes the test pass.
- Re-run the targeted test.
- Run the broader relevant test suite before committing.

Do not skip the red test step unless the change is documentation-only,
configuration-only, or otherwise cannot reasonably be tested.
