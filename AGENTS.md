# Repository working agreement

- After completing requested code or documentation work, run the relevant checks,
  commit all in-scope changes on the normal `main` branch, and push `origin/main`.
  Do this as the normal final checkpoint without waiting for a separate request.
- Do not create a topic branch or pull request for ordinary save-work checkpoints.
  Use those only when the user explicitly asks or direct `main` publication is
  genuinely unavailable.
- Treat commits and pushes as saved-work checkpoints, not as claims that a design
  is final, released, or beyond revision.
- Keep unrelated user changes, generated artifacts, credentials, and machine-local
  files out of the commit. If the intended scope is genuinely ambiguous, confirm it
  before staging.
- Do not open or merge a pull request unless the user asks for one or an active
  higher-priority workflow requires it. If pushing is blocked, report the exact
  blocker and leave the verified commit intact locally.
