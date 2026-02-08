**Contributing to Orbyx**

Thanks for your interest in contributing — I appreciate it. This document explains the preferred ways to report issues and submit changes so your contribution can be reviewed and merged quickly.

**Get started:**
- Fork the repository and create a branch named with a short, descriptive prefix, e.g. `bugfix/query-bug` or `feat/query-builder`.
- Install dependencies: `npm install`.
- Run tests: `npm run test`.
- Build locally when relevant: `npm run build`.

**Reporting bugs**
- Search existing issues first to avoid duplicates.
- Provide a clear title and include: a short description, steps to reproduce, expected vs actual behavior, and any relevant logs or stack traces.
- If the issue is about an integration or environment, include Node/OS versions and any config used.

**Proposing changes (Pull Requests)**
- Open a PR from your branch against `main` (or the branch noted in an issue).
- Keep changes focused and small; one logical change per PR.
- Include tests for bug fixes and new features. Place tests under the `tests/` folder and run `npm run test` locally before pushing.
- Ensure the project builds (`npm run build`) and tests pass.
- In the PR description, explain the motivation and include before/after examples where helpful.

**Commit messages & style**
- Use concise, imperative commit messages (e.g., "Add query builder helper").
- Follow existing project conventions for formatting and TypeScript usage.

**Code style & linting**
- Follow the repository's TypeScript configuration and formatting. Run any lint or format scripts the project provides (for example, `npm run lint` or `npm run format`) before submitting.

**Testing**
- Add or update tests for your changes. Tests live in `tests/` and are run with `npm run test`.

**Security & responsible disclosure**
- For security-sensitive issues, follow the guidance in SECURITY.md rather than opening a public issue. See [SECURITY.md](SECURITY.md) for details.

**Communication & review**
- Be responsive to review feedback; maintainers may request changes or ask clarifying questions.
- If your PR is large or experimental, open an issue first to gather feedback.

**Thank you**
I welcome contributions of all kinds — bug reports, tests, docs, and code. If you're unsure where to start, open an issue and I'll help you find something.
