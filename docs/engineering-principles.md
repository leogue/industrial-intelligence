# Engineering principles

We're a small team shipping a production app. Our number one rule is to keep things simple: handle the cases that matter, not every imaginable one. Simple code is easier to review, to secure, and to change.

- **Do what was asked, nothing more.** No extra features, refactors or "improvements" beyond the task. A bug fix doesn't need the surrounding code cleaned up. If you see something worth fixing elsewhere, mention it — don't do it.
- **Minimum code that solves the problem.** No abstraction, helper or config option for a single use; no design for hypothetical future needs. Three similar lines beat a premature abstraction. Would a senior engineer call it overcomplicated? Then simplify.
- **Validate at the boundaries, trust the inside.** Validate user input, external APIs and webhooks. Don't add fallbacks or error handling for situations that can't happen inside our own code — fail loudly instead of silently masking a bug.
- **Reuse before writing.** Look for an existing service, component or helper and follow the surrounding patterns. No new dependency without a reason stated in the PR.
- **Surgical diffs.** Every changed line traces back to the task. Don't reformat, rename or re-comment code you didn't need to touch. Delete dead code you created; leave pre-existing dead code alone unless asked.
- **Comments explain why, not what.** Only where the logic isn't self-evident.

## How to work

- **Read before you claim.** Never speculate about code you haven't opened.
- **Unclear spec, conflicting ADR, or a real design choice → stop and ask.** A wrong choice implemented costs more than a question. Otherwise, state your assumption and proceed.
- **Plan before multi-file changes.** If you can describe the diff in one sentence, just do it.
- **Prove it works.** Run the relevant tests / typecheck / lint before saying you're done, and show the result. For UI changes, check them in the browser.
- **Fix root causes.** Never silence an error, skip a test, add `# type: ignore` / `eslint-disable`, or hard-code a value to make a check pass. If a test seems wrong, say so instead of working around it.
