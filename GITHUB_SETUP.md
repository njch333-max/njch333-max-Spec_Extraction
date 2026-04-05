# GitHub Setup

This repo is prepared for a GitHub + Codex PR workflow.

## Recommended first-time setup
1. Create an empty GitHub repository named `Spec_Extraction` under your target owner.
2. Do **not** add a README, `.gitignore`, or license during repo creation.
3. Add the local SSH public key from:
   - `%USERPROFILE%\\.ssh\\spec_extraction_github_ed25519.pub`
4. Run the helper script:

```powershell
.\tools\connect-github-remote.ps1 -Owner <github-owner> -Repo Spec_Extraction
```

5. Push the current branch:

```powershell
git push -u origin master
```

## Recommended daily workflow
1. Start from updated `master`
2. Create a short-lived branch:

```powershell
.\tools\new-feature-branch.ps1 -Name fix/yellowwood-room-finalizer
```

3. Make changes
4. Run local verification
5. Push the branch
6. Open a GitHub PR
7. Use Codex for PR review against the GitHub diff

## When to ask Codex to fix vs review
- Default to `fix this bug` when the live bug is already visible, the source PDF has been checked, the wrong field/room/page is specific, and the expected outcome is clear.
- Prefer `review this PR` when the change touches shared parser flow, grouped-row cleanup, builder finalizers, or PDF QA state handling and you want regression review before merge.
- In practice for this project:
  - use `fix this bug` for focused issues such as appliance noise, missing overhead colours, truncated tap wording, or one-room fixture ownership
  - use `review this PR` for shared grouped-row changes, global room-retention rules, wet-area blacklist policy, and PDF QA workflow changes

## Review checklist
- Keep each PR focused on one builder, one field family, or one UI/workflow change.
- Call out affected builders/jobs in the PR body.
- Update `PRD.md`, `Arch.md`, `Project_state.md`, and `AGENTS.md` for major changes.
- Treat parser regressions, room-local ownership, PDF QA gating, and export/UI field drift as top review risks.
