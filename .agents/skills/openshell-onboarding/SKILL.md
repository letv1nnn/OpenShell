---
name: openshell-onboarding
description: "Structured, exercise-driven onboarding to the OpenShell codebase. Reads the engineer's GitHub profile to check Rust experience, generates a phased plan with hands-on exercises and quizzes covering architecture, sandbox model, policy engine, OCSF logging, and the full dev workflow (build from source, run gateway, create sandbox). Persists progress to onboarding-state.md. Offers Jira export. Triggers on 'onboard me to openshell', 'openshell onboarding', 'help me get started with openshell', 'new to openshell', '/openshell-onboarding'."
argument-hint: "[--github <username>]"
---

# OpenShell Onboarding Skill

Turns the first weeks on OpenShell into a sequenced, hands-on plan. Starts with a brief orientation, sets up the development environment, gets the engineer running locally, builds architectural understanding through Socratic exercises, then leads them through their first contribution.

## Content sourcing rule

This skill defines **behaviour, sequencing, decision logic, and interaction patterns**. It does NOT duplicate content from canonical docs. When a phase needs the engineer to learn a concept (architecture, policy model, compute runtimes, etc.), read the canonical source listed and synthesize a briefing from it. This keeps the skill small and prevents drift.

## Triggers

- "Onboard me to OpenShell"
- "Help me get started with OpenShell"
- "I just joined the OpenShell team"
- "New to OpenShell, where do I start?"
- "Continue my OpenShell onboarding"
- "What's my next exercise?"
- `/openshell-onboarding`

---

## Workflow

### Step 1 — Resolve GitHub username and check Rust experience

If the `--github <username>` argument was supplied, use it directly. Otherwise try `gh api user --jq .login`. If that returns a username, use it without asking. If `gh` is not authenticated or the command fails, ask the user for their GitHub username.

Once the username is known, check for real Rust experience (not just forked repos):

```bash
# 1. Non-fork repos with Rust code
gh api "users/{username}/repos?sort=pushed&per_page=100" --jq '[.[] | select(.fork == false) | select(.language == "Rust")] | length'

# 2. Recent Rust commits (last year)
# Portable one-year cutoff: try GNU date, fall back to BSD/macOS date
CUTOFF=$(date -d '1 year ago' +%Y-%m-%d 2>/dev/null || date -v-1y +%Y-%m-%d)
gh api "search/commits?q=author:{username}+language:rust+committer-date:>$CUTOFF&per_page=5" --jq '.total_count'

# 3. Rust PRs authored
gh api "search/issues?q=author:{username}+language:rust+type:pr&per_page=5" --jq '.total_count'
```

**Binary decision — output one line, nothing more:**

| Signal | Output | Action |
|--------|--------|--------|
| Non-fork Rust repos OR ≥5 Rust commits/PRs | `Rust: experienced` | Skip generic Rust basics in Phase 1c. Point to OpenShell-specific patterns only. |
| None of the above | `Rust: new — study track included` | Include Phase 1c as a Rust ramp-up with curated resources. |

Do not narrate the search results or analysis. State the result in one line and proceed.

---

### Step 2 — Check for existing progress

Resolve the repository root once (`git rev-parse --show-toplevel`) and look for `<repo-root>/onboarding-state.md`. Use this same canonical path for every read and write — never the current working directory.

If found: load state, show current phase and completed/blocked items, ask to continue or regenerate.

If not found: generate from scratch.

---

### Step 3 — Build the phased plan

---

#### Phase 0: What OpenShell Is

Present this as a concise briefing. Do not ask the engineer to read anything yet — just give them the mental model they need to make sense of everything that follows.

**Source:** Read `architecture/README.md` and `README.md`. Synthesize a spoken briefing covering:
- What OpenShell is (one-paragraph identity)
- The four main components: Gateway, Supervisor+Proxy, Providers, Policies
- How `inference.local` works
- The gateway ↔ sandbox relationship (control plane vs runtime enforcement)

Keep it concise — aim for a briefing someone can absorb in 2 minutes, not a doc dump.

**Tasks:**
- [ ] Read `README.md` — project quickstart and context
- [ ] Read `CONTRIBUTING.md` — PR conventions, commit format (Conventional Commits + DCO), vouch system
- [ ] Identify key contacts: `git shortlog -sn --no-merges | head -10`

> **Note:** Security vulnerabilities must never be filed as GitHub issues — report them privately per `SECURITY.md`.

---

#### Phase 1a: Development Environment Setup

Goal: dev toolchain installed, project builds from source.

↳ **Blocks:** Phase 1b (running locally)

**Source:** Read the `## Prerequisites` section of `CONTRIBUTING.md`. Walk through each requirement step by step, waiting for confirmation before moving on.

**Guided flow:**

1. **mise** — Install and activate for their shell. Verify: `mise --version`
2. **Rust** — Verify installed version meets minimum. If missing, install via rustup. Verify: `rustc --version`
3. **Python** — Verify version meets minimum. Verify: `python3 --version`
4. **Docker or Podman** — Confirm which container runtime they have and that it's running. Verify: `docker info` or `podman info`
5. **Z3** — Install for their platform. Verify: `pkg-config --modversion z3` or `z3 --version`
6. **macOS only** — Check for Apple Command Line Tools: `xcode-select -p`. If missing or protobuf-src build errors occur, guide through install/reinstall.
7. **Trust and build** — `mise trust` then `cargo build` (or `mise run build` if available). First build will take several minutes.

After each step, ask the engineer to confirm success before moving to the next.

**Verification:** `mise run pre-commit` passes (proves toolchain is functional).

---

#### Phase 1b: Get OpenShell Running Locally

Goal: gateway running, sandbox created, first hands-on feel for the product.

↳ **Blocks:** all subsequent phases

**Before diving in, present this brief primer on core primitives:**

| Primitive | What it is | What you'll do with it |
|-----------|-----------|----------------------|
| **Gateway** | Control-plane server — manages sandboxes, providers, policies, inference config. One per OpenShell instance. | Start it, check its status |
| **Sandbox** | Isolated container where an agent runs. Supervisor (PID 1) enforces policy; inline proxy intercepts all network traffic. | Create one, connect into it, run commands |
| **Provider** | Credential bundle. Real secrets stay outside the sandbox; the proxy swaps dummy tokens for real ones on the wire. | Optionally attach one so the sandbox can call an LLM |
| **Policy** | Declarative YAML rules — filesystem, network (per-host, per-method, per-binary), process, inference. Default: deny all. | See policy enforcement in action, hot-reload a rule |
| **`inference.local`** | Virtual hostname inside a sandbox — routes to gateway-managed model backends. | Use it to call Claude from inside a sandbox |

Phase 2 explains *why* each works the way it does. Right now you just need to know *what* they are.

**Start by asking:** "Do you want to try OpenShell locally? I'll walk you through it step by step."

If yes — follow this guided flow. Present one step at a time, wait for confirmation before moving to the next.

---

**Step 1: Run the gateway from source**

**Source:** Read `CONTRIBUTING.md` "Getting Started" section for the `mise run gateway` workflow.

```bash
mise run gateway
```

Ask: "Run `openshell status`. What do you see?"

If they prefer to install the binary instead of running from source:

```bash
# macOS (recommended)
brew tap nvidia/openshell
brew install openshell

# Universal
curl -LsSf https://raw.githubusercontent.com/NVIDIA/OpenShell/main/install.sh | sh

# pip/uv
uv tool install -U openshell
```

---

**Step 2: Fix the Podman problem (macOS only — skip if status is already Connected)**

If they hit:
```
Error: configuration error: no compute driver configured and auto-detection found
no suitable driver; set --drivers or OPENSHELL_DRIVERS to kubernetes, podman, docker, or vm
```

This is a known macOS issue ([#1834](https://github.com/NVIDIA/OpenShell/issues/1834)). Two workarounds — ask which container runtime they have (Podman or Docker) and give the matching recipe:

*Podman:*
```bash
podman machine start
mkdir -p ~/.config/openshell

# The real socket is under /var/folders — NOT what `podman info` reports
SOCK=$(find /var/folders -name "podman-machine-default-api.sock" 2>/dev/null | head -1)

cat > ~/.config/openshell/gateway.env << EOF
OPENSHELL_DRIVERS=podman
OPENSHELL_PODMAN_SOCKET=$SOCK
EOF

brew services restart openshell
sleep 3
openshell status
```

*Docker (or Podman in Docker-compat mode):*
```bash
mkdir -p ~/.config/openshell

cat > ~/.config/openshell/gateway.toml << 'TOML'
[openshell.gateway]
bind_address = "0.0.0.0:17670"

[openshell.drivers.docker]
host_gateway_ip = "192.168.127.254"
TOML

cat > ~/.config/openshell/gateway.env << 'ENV'
OPENSHELL_BIND_ADDRESS=0.0.0.0
OPENSHELL_GATEWAY_CONFIG=/Users/YOURUSERNAME/.config/openshell/gateway.toml
ENV

brew services restart openshell
sleep 3
openshell status
```

Replace `YOURUSERNAME` with their macOS username. Target: `Status: Connected`.

---

**Step 3: Create a sandbox**

Ask what API keys they have available (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, or GCP/Vertex). Then guide accordingly:

*Anthropic or OpenAI key:*
```bash
openshell sandbox create -- claude   # or: -- codex
# When prompted to create a provider from local credentials: yes
```

*GCP / Vertex AI (Claude via GCP):*

**Source:** Read `architecture/google-vertex-ai-provider.md` for the full provider setup flow.

```bash
# Create the provider
openshell provider create \
  --name vertex-prod \
  --type google-vertex-ai \
  --from-gcloud-adc \
  --config VERTEX_AI_PROJECT_ID=YOUR_PROJECT_ID \
  --config VERTEX_AI_REGION=global

# Enable provider injection
openshell settings set --global --key providers_v2_enabled --value true --yes

# Set inference routing
openshell inference set --provider vertex-prod --model claude-sonnet-4-6
# If verification fails: add --no-verify

# Create sandbox
openshell sandbox create --provider vertex-prod
```

Then connect and launch Claude Code:
```bash
openshell sandbox connect YOUR_SANDBOX_NAME

# Inside the sandbox:
ANTHROPIC_BASE_URL="https://inference.local" \
ANTHROPIC_API_KEY=unused \
CLAUDE_CODE_DISABLE_EXPERIMENTAL_BETAS=1 \
claude --bare
```

> Do NOT set `CLAUDE_CODE_USE_VERTEX=1` inside the sandbox — it bypasses the gateway and fails because the sandbox has no GCP credentials.

*No keys yet:*
```bash
openshell sandbox create --from base -- sleep infinity
```

---

**Step 4: Explore essential commands**

```bash
openshell status                             # Gateway health
openshell sandbox list                       # All sandboxes
openshell sandbox get <name>                 # Sandbox details
openshell sandbox connect <name>             # SSH into sandbox
openshell term                               # TUI dashboard (k9s-style)
openshell sandbox create --output json       # Machine-readable output
```

---

**Exercise 1 — Security Layer Walkthrough**

Now run each command below against a live sandbox. After each one, before moving to the next, ask: *"Which component enforced that? Proxy, Landlock, or seccomp?"* Let them answer — confirm or correct.

```bash
openshell sandbox create --name sec-test
```

*SSRF — cloud metadata blocked:*
```bash
openshell sandbox exec --name sec-test -- curl -s --max-time 5 http://169.254.169.254/latest/meta-data
# Expected: {"error":"policy_denied"}
```

*Default deny:*
```bash
openshell sandbox exec --name sec-test -- curl -s --max-time 5 https://example.com
# Expected: exit code 56 (proxy reset)
```

*Filesystem isolation:*
```bash
openshell sandbox exec --name sec-test -- touch /usr/bin/evil
# Expected: Permission denied
```

*Seccomp:*
```bash
openshell sandbox exec --name sec-test -- python3 -c "import socket; socket.socket(socket.AF_PACKET, socket.SOCK_RAW)"
# Expected: PermissionError

openshell sandbox exec --name sec-test -- unshare -U whoami
# Expected: Operation not permitted
```

*Policy hot-reload — no sandbox restart:*
```bash
openshell sandbox exec --name sec-test -- curl -s --max-time 5 https://api.github.com
# Expected: blocked

openshell policy update sec-test \
  --add-endpoint api.github.com:443:read-only:rest:enforce \
  --add-allow 'api.github.com:443:GET:/**' --wait

openshell sandbox exec --name sec-test -- curl -s --max-time 5 https://api.github.com | head -3
# Expected: {"current_user_url":"https://api.github.com/user", ...}

# POST still denied (L7 read-only rule)
openshell sandbox exec --name sec-test -- curl -s --max-time 5 -X POST https://api.github.com/repos
# Expected: {"error":"policy_denied","layer":"l7","method":"POST"}
```

Cleanup: `openshell sandbox delete sec-test`

The question after each command ("which component enforced that?") is not rhetorical — it seeds the mental model that Phase 2 builds on.

---

#### Phase 1c: Rust Track *(parallel with 1a/1b)*

**If Rust: experienced** — no exercises. Read `Cargo.toml` files across `crates/` to identify the key frameworks and libraries used in OpenShell. Present a summary table mapping each major dependency to its role and where it appears (async runtime, gRPC, CLI parsing, error handling, serialization, HTTP/TLS, TUI, logging).

**If Rust: new** — learn the language fundamentals before diving into OpenShell code:

1. **Read [The Rust Programming Language](https://doc.rust-lang.org/book/)** — chapters 1–10 cover ownership, structs, enums, error handling, generics, and traits. These are essential for reading any OpenShell code.
2. **Complete [Rustlings](https://github.com/rust-lang/rustlings)** — small exercises that fix compile errors. Builds muscle memory for the compiler's feedback loop.

Once comfortable with ownership, traits, and `Result` handling, move on to Phase 2 — OpenShell-specific patterns (async, gRPC, etc.) are learned in context.

---

#### Phase 2: Architecture Deep Dive

Goal: build a mental model of how the system is structured and why each component exists.

↳ Blocked by: Phase 1b

**Source:** First **present the reading list below to the engineer verbatim** — the exact doc paths, so they know which files to open. Then read each doc yourself and present the key concepts before they read it themselves, so they know what to look for. Do not skip showing the paths: the engineer must see the full list of files, not just your synthesized briefing.

Reading list (show these paths to the engineer):

- `architecture/README.md` — overall system architecture and core boundaries
- `architecture/gateway.md` — control plane, gRPC API, auth model
- `architecture/sandbox.md` — runtime model, trust levels, startup sequence, isolation layers
- `architecture/security-policy.md` — policy decision flow, evaluation order
- `architecture/compute-runtimes.md` — driver abstraction, runtime backends
- `architecture/build.md` — build system, toolchain pinning, cross-compilation

Read all six before synthesizing. Do not truncate to a subset.

Also run: `grep -h '^description' crates/*/Cargo.toml` for a quick crate-to-purpose reference.

**Deployment options:** OpenShell supports local (Docker/Podman), Kubernetes, and OpenShift deployments. For Kubernetes setup, read `docs/kubernetes/setup.mdx`. For OpenShift-specific deployment (SCC binding, chart overrides), read `docs/kubernetes/openshift.mdx`. Point the engineer to the relevant docs based on their deployment interest.

**Exercise 2 — Crate Ownership:**

For each crate below, answer two questions:
1. What does this crate own? (one sentence — its job, not its implementation)
2. If this crate had a bug, what user-visible behavior would break?

Crates to cover: `openshell-server`, `openshell-sandbox`, `openshell-policy`, `openshell-supervisor-network`, `openshell-sdk`.

Use only the architecture docs and `Cargo.toml` files. No deep code reading required yet.

**Exercise 3 — Request Trace:**

Trace the path of `openshell sandbox create` from CLI invocation to sandbox workload running.

For each handoff point, provide `file:line`:
- CLI crate entry point
- SDK call that leaves the process
- gRPC service handler in the gateway
- Compute driver invocation
- Supervisor startup inside the workload

Agent will verify all `file:line` references exist and form a coherent path.

**Exercise 4 — Architecture in Your Own Words:**

Explain OpenShell's architecture as if you were onboarding someone else. No diagrams — speak or write it out. Cover: what the gateway does, what happens when a sandbox is created, how network traffic is handled, how credentials reach the agent.

As you explain, the skill will ask follow-up questions based on what you mention. Examples:
- If you mention the proxy: *"You mentioned the proxy intercepts TLS — how does it do that without breaking certificate validation for the agent?"*
- If you mention providers: *"How does the proxy know which outgoing request to substitute a credential into?"*
- If you mention Landlock: *"Filesystem policy is set at creation and cannot be loosened — what's the architectural reason for that constraint?"*
- If you mention the gateway not enforcing egress: *"Why is that decision made in the sandbox instead of centrally at the gateway?"*

The goal is to surface gaps in the mental model and fill them — not to test recall.

---

#### Phase 3: Sandbox & Policy Deep Dive

Goal: understand the core product deeply enough to write policies and reason about enforcement.

↳ Blocked by: Phase 2

**Source:** Read the following and synthesize the key concepts for the engineer:
- `architecture/sandbox.md` — full read (supervisor startup sequence, all five isolation layers)
- `architecture/security-policy.md` — full read (evaluation order, enforcement layers)
- `docs/reference/policy-schema.mdx` — field-by-field YAML policy reference
- `crates/openshell-ocsf/src/` — Browse builder types to understand OCSF structured logging vs plain tracing. Read `AGENTS.md` "Sandbox Logging (OCSF)" section for the guidelines.

**Exercise 5 — Sandbox Startup Trace:**

Read `crates/openshell-sandbox/src/` and trace the supervisor startup sequence from `main` through the six steps described in `architecture/sandbox.md`. For each step, provide `file:line`.

**Exercise 6 — Write a Policy:**

Using the `generate-sandbox-policy` skill (or by hand), write a sandbox network policy that:
- Allows `GET /api/v1/messages` and `POST /api/v1/messages` to `api.anthropic.com:443` with L7 inspection
- Denies all other outbound traffic
- Uses `enforce` mode (not `audit`)

Apply it to a running sandbox. Verify allowed traffic works and denied traffic is blocked.

**Exercise 7 — OCSF Event:**

Find an existing `ocsf_emit!()` call in `crates/openshell-sandbox/`. Explain: which builder it uses, what severity it assigns, and why (using the OCSF guidelines in `AGENTS.md`). Then identify one place in the same crate that uses `warn!()` where an OCSF `DetectionFindingBuilder` would be more appropriate. Explain your reasoning.

**Quiz — Architecture:**

> 1. A sandboxed agent tries to open a raw socket. What prevents it, and at which isolation layer?
> 2. Network policy is hot-reloadable but filesystem policy is not. What is the architectural reason?
> 3. `inference.local` is configured through gateway inference settings, not network policy. Why?
> 4. When should you use `ocsf_emit!()` instead of `warn!()`? Give two concrete criteria.
> 5. What is "trust-on-first-use binary identity" in the proxy? What attack does it prevent?

---

#### Phase 4: First Contribution

Goal: complete the full contribution cycle — issue to merged PR.

↳ Blocked by: Phase 3

**Source:** Read `CONTRIBUTING.md` for branch naming, commit format, PR template, DCO signoff, and the vouch system.

**Tasks:**
- [ ] Install the pre-commit hook: `mise generate git-pre-commit --write --task=pre-commit`
- [ ] Find a suitable issue (see below)
- [ ] Implement, write/update tests, open PR using `create-github-pr` skill
- [ ] Address review feedback; get approval or merge

**Finding a suitable issue:**

Run this to get candidates:

```bash
gh issue list \
  --repo NVIDIA/OpenShell \
  --label "good first issue" \
  --state open \
  --limit 20 \
  --json number,title,comments,assignees \
  --jq '.[] | select(.assignees | length == 0) | select(.comments | length == 0) | {number, title}'
```

Filter criteria:
- No assignees (not claimed)
- No comments (nobody has publicly expressed intent to take it — a comment saying "I'll work on this" is a soft claim even without assignment)
- Label: `good first issue`

If that returns nothing, broaden to issues with 0 assignees and ≤ 1 comment, then manually check that the comment is not someone claiming the issue:

```bash
gh issue list \
  --repo NVIDIA/OpenShell \
  --label "good first issue" \
  --state open \
  --limit 20 \
  --json number,title,comments,assignees \
  --jq '.[] | select(.assignees | length == 0) | select(.comments | length <= 1) | {number, title, comments}'
```

Present the filtered list to the engineer with title and number. Let them pick. Before they start, run:

```bash
gh issue view <number> --repo NVIDIA/OpenShell --comments
```

Confirm no comment says "I'm working on this", "assigned to me", "taking this", or similar. If clear — proceed.

**Exercise 8 — Micro-PR:**

Complete the chosen issue. PR must:
- Follow Conventional Commits format: `type(scope): description`
- Include `--signoff` on every commit (DCO)
- Pass `mise run pre-commit`
- Include at least one test
- Fill out the PR template: Summary, Related Issue, Changes, Testing, Checklist

**Vouch reminder:** if you are an external contributor, your PR requires a `/vouch` from a maintainer before it can merge.

Verification: PR merged or approved with feedback addressed.

---

### Step 4 — Offer Jira export

After presenting the full plan, use `AskUserQuestion` with these options:

```
Question: "Create this plan as Jira tickets?"
Options:
  - "Yes — create Jira tickets"
  - "Save to file only (onboarding-state.md)"
  - "No thanks, display only"
```

**Jira creation flow (requires Jira MCP):**

1. Ask for the Jira project key if not already known.
2. Create one Epic: `OpenShell Onboarding: [Name] — [start date]`; description: GitHub username, Rust background.
3. For each phase, create a Story with the full task list, exercises, and quiz questions.
   - Story points: Phase 0=1, Phase 1a=3, Phase 1b=5, Phase 1c=3, Phase 2=5, Phase 3=8, Phase 4=8
4. For each exercise, create a sub-task with instructions and verification criteria.
5. Set `blockedBy` links matching the phase dependency graph.
6. Assign to user's Jira account if known.
7. Print: `Created 1 Epic, N Stories, M sub-tasks in project [KEY].`

If Jira MCP unavailable: say so, fall back to display-only + file save.

---

### Step 5 — Persist progress

Write (or update) `<repo-root>/onboarding-state.md` — the same canonical path resolved in Step 2 (`git rev-parse --show-toplevel`). Never write to the current working directory.

```markdown
# OpenShell Onboarding State
Generated: <date>
GitHub: <username> | Rust: <experienced / new>

## Progress

### Phase 0: What OpenShell Is
- [x] Read README.md, CONTRIBUTING.md
- [x] Identified key contacts

### Phase 1a: Development Environment Setup
- [ ] mise installed and activated
- [ ] Rust, Python, Docker/Podman verified
- [ ] Z3 installed
- [ ] Project builds from source
- [ ] `mise run pre-commit` passes

### Phase 1b: Get OpenShell Running Locally
- [ ] Gateway running (Status: Connected)
- [ ] First sandbox created
- [ ] Exercise 1 (Security Layer Walkthrough): PENDING

### Phase 1c: Rust Track
*(OpenShell patterns only — Rust: experienced)*

...

## Blockers
- <any pending access or dependencies>

## Next Action
Complete Phase 1a (Development Environment Setup).
```

Update after every verified exercise or quiz.

---

### Step 6 — Coach mode

After presenting the plan and handling Jira/file, use `AskUserQuestion`:

```
Question: "Ready to start?"
Options:
  - "Start from the beginning (Phase 0)"
  - "Jump to dev environment setup (Phase 1a)"
  - "Jump straight to hands-on (Phase 1b — run OpenShell locally)"
  - "I'll work through it on my own"
```

Then proceed based on their selection.

On Exercise 4 (Architecture in Your Own Words): actively listen to the engineer's explanation. When they mention a component, ask a follow-up question that probes one level deeper. Do not lecture — ask. Let the engineer reason it out. Only correct if they are wrong, and then ask "what would you expect to happen if..." to confirm understanding.

On all other exercises:
1. Present the task with explicit verification criteria
2. Let the user attempt
3. Validate the output
4. Record completion in `onboarding-state.md`
5. Advance to next item

When the user says "continue my onboarding" or "next exercise":
1. Read `<repo-root>/onboarding-state.md` (canonical path from Step 2)
2. Find the first incomplete item
3. Present it and coach through it

### Step 7 — Phase completion summary

After each phase:

```
Phase <N> complete — <X> exercises done, <Y> quiz questions answered.
Next: Phase <N+1> — <name>. First task: <specific task>.
```

---

## Design rules

**Content sourcing:** Read canonical docs (`architecture/`, `CONTRIBUTING.md`, `docs/`, `AGENTS.md`) at runtime — never hardcode concepts or architecture descriptions that may change. The skill defines what to teach and how, not the content itself.

**Quizzes:** architecture and design reasoning only. Ask *why* and *what breaks*, not trivia. Pass threshold: 70% — on fail, point to specific doc section, allow retry.

**Exercises:** every exercise produces a concrete artifact (command output, policy YAML, `file:line` table, or explanation). No exercise ends with "read and understand."

**Exercise 4 (Socratic architecture):** ask follow-up questions based on what the engineer actually says. Do not follow a script. Surface gaps, do not fill them immediately — ask questions that help the engineer arrive at the answer.

**Output:** be terse. One-line status updates. No narration of what you are about to do.

## Common mistakes to avoid

| Mistake | Fix |
|---------|-----|
| Skipping GitHub profile check | Always check Rust repos before asking about experience |
| Making Phase 1c mandatory | Only include full Rust ramp-up if no prior Rust repos found |
| Narrating analysis | State result in one line, move on |
| Generic exercises | Use real OpenShell file paths, real commands, real crate names |
| Scripted Socratic questions | In Exercise 4, respond to what the engineer actually said, not a fixed list |
| Forgetting DCO signoff | Every commit needs `--signoff`; pre-commit does not check this |
| Hardcoding doc content | Read from canonical source at runtime; never paste architecture descriptions into responses verbatim from this skill file |
| Skipping dev environment setup | Phase 1a must complete before trying to build/run; don't jump to Phase 1b |
