# Issue tracker: GitHub

Issues and specs live in https://github.com/yb2915722928-cpu/-ETL-Agent-.
Use the `gh` CLI. Run inside this repo, or pass
`--repo yb2915722928-cpu/-ETL-Agent-` explicitly.

## Conventions

- Create: `gh issue create --title "..." --body-file <file>`.
- Read: `gh issue view <number> --json number,title,body,labels,comments`.
- List: `gh issue list --state open --json number,title,body,labels,comments`,
  adding label and state filters as needed.
- Comment: `gh issue comment <number> --body-file <file>`.
- Labels: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`.
  Choose labels according to the skill being run; triage vocabulary is in
  `triage-labels.md`.
- Close: `gh issue close <number> --comment "..."`.
- Parent/child: use native GitHub sub-issues. With gh 2.94+, use
  `gh issue create --parent <parent> ...` or
  `gh issue edit <parent> --add-sub-issue <child>`.
  With older gh, POST to `repos/<owner>/<repo>/issues/<parent>/sub_issues`
  with `sub_issue_id=<child database id>`.
  If sub-issues are unavailable, put `Part of #<parent>` at the top of the
  child body and add the child to a task list in the parent.

## Pull requests as a triage surface

**PRs as a request surface: no.**

GitHub issues and pull requests share a number space. Resolve a reference
with `gh pr view <number>` and fall back to `gh issue view <number>`.

## Skill requests

- "Publish to the issue tracker": create a GitHub issue.
- "Fetch the relevant ticket": read the GitHub issue.

## Wayfinding operations

- Map: one issue labelled `wayfinder:map`, containing the map's notes,
  decisions and fog.
- Child ticket: link it to the map; use its matching type label:
  `wayfinder:research`, `wayfinder:prototype`, `wayfinder:grilling`,
  or `wayfinder:task`. Assign the driving developer when claimed.
- Blocking: prefer native issue dependencies. POST to
  `repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by` with
  `issue_id=<blocker database id>`. Obtain the database id with
  `gh api repos/<owner>/<repo>/issues/<number> --jq .id`.
  If dependencies are unavailable, use a `Blocked by: #<n>, #<n>` line.
- Frontier: inspect the map's open children in map order; choose the first
  unassigned child with no open blockers. Native
  `issue_dependencies_summary.blocked_by` counts open blockers.
- Claim: `gh issue edit <number> --add-assignee @me` as the first write.
- Resolve: record the answer, close the child, then append a gist and link
  to the map's decisions.
