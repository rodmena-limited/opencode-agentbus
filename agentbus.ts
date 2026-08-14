/**
 * AgentBus plugin for opencode — the WAKE.
 *
 * WHY THIS FILE EXISTS AT ALL, given the MCP server was already configured:
 *
 * MCP is PULL-ONLY. It hands the agent tools to ASK for mail; nothing pushes.
 * With only the MCP server wired, a peer's message lands in the inbox and the
 * session never learns of it — which is exactly what happened: bob's "wake-up
 * ping" sat unread in frontend-builder's inbox while the session idled, because
 * nothing was listening. An inbox you must remember to poll is not a wake path.
 *
 * PARITY WITH THE CLAUDE CODE PLUGIN BY SHARING ITS CLIENT, NOT COPYING IT.
 * Protocol, guard rules and provenance wording live in ONE implementation
 * (`agentbus` / `agentbus-hook`), so a fix reaches both harnesses at once. Only
 * the harness-specific part — how a message becomes a turn — lives here.
 *
 * THE BRIDGE IS THE WHOLE POINT. The first cut of this plugin spawned
 * `agentbus watch --exec agentbus-hook notify`, which is correct on Claude Code
 * (a socket carries it into the session) and INERT on opencode: `notify` wrote
 * to a side channel and nothing ever called back into the session. It would
 * have loaded, run, logged nothing and woken no one — indistinguishable from
 * working. Here the plugin consumes the stream ITSELF and calls promptAsync.
 */
import type { Plugin, PluginInput, Hooks } from "@opencode-ai/plugin"

const HOOK_BIN = process.env.AGENTBUS_HOOK_BIN || "agentbus-hook"
const CLI_BIN = process.env.AGENTBUS_BIN || "agentbus"

type HookResult = { code: number; stdout: string; stderr: string }

/**
 * Run the shared client. NEVER throws — a messaging plugin that breaks the
 * session it is attached to is worse than one that stays quiet. The ONE
 * exception is the gate, which converts failure into a DENY at its call site,
 * because a gate that cannot reach its rules must not permit.
 */
/** Strip JSONC comments and trailing commas so JSON.parse accepts the file.
 *
 * STRING-AWARE ON PURPOSE. opencode configs are full of URLs — bob's has
 * `https://runflow.rodmena.app/mcp` — and a naive `//` strip would amputate every
 * one of them into `https:`, producing a config that parses cleanly and is
 * silently wrong. That is worse than failing to parse, because a wrong endpoint
 * looks like a working install that cannot reach the bus.
 */
function stripJsonc(text: string): string {
  let out = ""
  let inStr = false, esc = false, line = false, block = false
  for (let i = 0; i < text.length; i++) {
    const c = text[i], n = text[i + 1]
    if (line) { if (c === "\n") { line = false; out += c } continue }
    if (block) { if (c === "*" && n === "/") { block = false; i++ } continue }
    if (inStr) {
      out += c
      if (esc) esc = false
      else if (c === "\\") esc = true
      else if (c === '"') inStr = false
      continue
    }
    if (c === '"') { inStr = true; out += c; continue }
    if (c === "/" && n === "/") { line = true; i++; continue }
    if (c === "/" && n === "*") { block = true; i++; continue }
    out += c
  }
  // Trailing commas are legal in JSONC and fatal to JSON.parse.
  return out.replace(/,(\s*[}\]])/g, "$1")
}

async function run(
  $: PluginInput["$"], bin: string, args: string[], stdin?: string,
  env?: Record<string, string>,
): Promise<HookResult> {
  try {
    // THE GATE MUST BE ABLE TO REACH ITS RULES.
    //
    // Without a credential the guard cannot even determine whether an action
    // needs approval, so it fails closed and denies EVERYTHING — including
    // reads. A session in that state reports "zero bus visibility", which reads
    // like a permissions decision and is actually a missing key. opencode holds
    // the right key in its own MCP config, so it is passed through here rather
    // than left to the host's global config, which belongs to another agent.
    const e = env ? { ...process.env, ...env } : undefined
    const base = stdin === undefined
      ? $`${bin} ${args}`
      : $`echo ${stdin} | ${bin} ${args}`
    const proc = (e ? base.env(e) : base).quiet().nothrow()
    const out = await proc
    return {
      code: out.exitCode ?? 0,
      stdout: out.stdout?.toString() ?? "",
      stderr: out.stderr?.toString() ?? "",
    }
  } catch (err) {
    // Absent or unrunnable binary: report non-zero so the gate treats it as
    // undecidable rather than as permission.
    return { code: 127, stdout: "", stderr: String(err) }
  }
}

export const server: Plugin = async (input: PluginInput): Promise<Hooks> => {
  const { $, client } = input

  // LOAD MARKER. A plugin that never loads and one that loads and does nothing
  // produce identical output — silence. Without this, every assertion about the
  // wake rests on an unobserved premise, which is the exact defect that has
  // bitten this platform repeatedly. Written unconditionally, not behind an env
  // var, so "did it load" is always answerable after the fact.
  // ONE PLACE DECIDES WHERE CONFIG LIVES, AND IT HONOURS AGENTBUS_CONFIG_DIR.
  //
  // This was hardcoded to $HOME/.config/agentbus in two places while the Python
  // client and the monitor both honour AGENTBUS_CONFIG_DIR. bob reported it as a
  // testability defect and he is right that it is not cosmetic: with the env var
  // ignored, a probe cannot give this plugin an isolated config, so every test
  // either touches the real credential store or cannot exercise the credential
  // path at all. A component that cannot be pointed at a fixture is a component
  // whose failures are only discoverable in production.
  const configDir = process.env.AGENTBUS_CONFIG_DIR
    || `${process.env.HOME}/.config/agentbus`
  const markerPath = process.env.AGENTBUS_PLUGIN_MARKER
    || `${configDir}/opencode-plugin.marker`
  async function mark(line: string): Promise<void> {
    try {
      const prev = await Bun.file(markerPath).text().catch(() => "")
      await Bun.write(markerPath, prev + line + "\n")
    } catch { /* diagnostics only; never break a session for it */ }
  }
  await mark(`loaded ${new Date().toISOString()} `
    + `env.AGENTBUS_AGENT=${process.env.AGENTBUS_AGENT ?? "<unset>"} `
    + `env.AGENTBUS_API_KEY=${process.env.AGENTBUS_API_KEY ? "<present>" : "<unset>"} `
    + `pid=${process.pid}`)

  const seenSessions = new Set<string>()
  let watcher: { kill: () => void } | undefined
  let agentName = ""
  // Resolved once at load and reused: the gate, the catch-up hooks and the
  // watcher must all act as the SAME agent, or a session enforces one identity's
  // rules while reading another's mail.
  let sessionKey = ""

  // THE SESSION MAY NOT EXIST YET, AND THE WAKE CANNOT WAIT FOR IT.
  //
  // An idle opencode sitting at "Ask anything..." has NO session: `event` has
  // never fired. Starting the watcher from the session hook therefore means the
  // agent begins listening only once the human has already typed — which is the
  // one moment a wake is not needed. That is the reported failure: a peer's
  // message landed, the session idled, nothing happened.
  //
  // So the wake starts at LOAD, and messages arriving before a session exists
  // are surfaced on the session-independent TUI surfaces — and buffered ONLY if
  // that fails, since submitPrompt already sends them.
  let currentSession = ""
  const pending: string[] = []

  // NEVER ANNOUNCE THE SAME DELIVERY TWICE. Cheap, and it makes a duplicate
  // stream (two plugin processes, a replayed cursor) harmless instead of
  // doubling every wake.
  const announced = new Set<string>()

  // BULKHEAD. The loop above was a logic bug, and logic bugs recur; this is the
  // structural stop that bounds the blast radius of the NEXT one. At most
  // MAX_INJECTIONS in a rolling window, after which the plugin goes quiet and
  // says so rather than flooding the client into uselessness.
  const MAX_INJECTIONS = 12
  const WINDOW_MS = 60_000
  let injections: number[] = []
  let mutedUntil = 0
  // One greeting per PROCESS, for the reason above.
  let greeted = false
  function budgetAllows(now: number): boolean {
    if (now < mutedUntil) return false
    injections = injections.filter((t) => now - t < WINDOW_MS)
    if (injections.length >= MAX_INJECTIONS) {
      mutedUntil = now + WINDOW_MS
      return false
    }
    injections.push(now)
    return true
  }

  /**
   * The credential this session acts with, taken from the SAME place the MCP
   * server reads it: opencode's own config.
   *
   * Not from $AGENTBUS_API_KEY, and emphatically not from the host's global
   * agentbus config. This host runs several agents; the global config belongs
   * to `agentbus-dev`. An earlier build fell back to it and started streaming
   * agentbus-dev's mail into a session that was supposed to be
   * frontend-builder — a session silently consuming another agent's wakes,
   * which is worse than not waking at all. The MCP header is the one credential
   * this session has actually been given, so CLI and MCP agree by construction.
   */
  async function resolveKey(): Promise<string> {
    if (process.env.AGENTBUS_API_KEY) return process.env.AGENTBUS_API_KEY

    // THIRD SOURCE, found by david: the host's bound-key store. `agentbus
    // sibling add` writes ~/.config/agentbus/keys/<agent>.env with a key bound
    // to that agent alone, and the shell hooks already source it — but this
    // plugin never looked, so a host with a perfectly good credential on disk
    // still reported "no credential".
    //
    // ONLY when AGENTBUS_AGENT names the identity. If several key files exist,
    // picking one would be guessing which agent this session is, and SPECS/0029
    // is explicit that identity is declared and never inferred: a wrong guess
    // makes this session act as, and read the mail of, somebody else. Staying
    // unidentified is the safe failure.
    const named = process.env.AGENTBUS_AGENT
    if (named) {
      try {
        const p = `${configDir}/keys/${named}.env`
        const text = await Bun.file(p).text()
        const m = text.match(/^\s*(?:export\s+)?AGENTBUS_API_KEY\s*=\s*["']?([^"'\s]+)/m)
        if (m) {
          await mark(`credential from the bound-key store for ${named}`)
          return m[1]
        }
      } catch { /* no key file for this agent — fall through */ }
    }
    try {
      // PROJECT-LEVEL FIRST, THEN GLOBAL — the order is the point, and it is
      // what #83's "no-bypass leg" rests on.
      //
      // `agentbus setup opencode` writes a PROJECT-scoped opencode.json with the
      // agent's own bound key in mcp.agentbus. A project that declares its own
      // identity must be served by that identity — NOT by whatever the global
      // config happens to hold, which belongs to the host and can be a DIFFERENT
      // agent's credential. So a project-level entry wins outright.
      //
      // The reverse also matters: a project with NO project-level entry falls
      // back to the global config — that is the historical behaviour, and it is
      // how an unwired directory in a wired host still gets its MCP tools. But a
      // wired project's gate leg comes from ITS OWN declared identity, which is
      // what makes the opt-in per-session rather than per-host (SPECS/0041).
      //
      // opencode merges project and global config (verified: a project
      // opencode.json `plugin` entry coexists with the global plugin array), so
      // reading them separately here mirrors what the harness itself resolves.
      const configs = [
        `${process.env.PWD}/opencode.jsonc`,
        `${process.env.PWD}/opencode.json`,
        `${process.env.HOME}/.config/opencode/opencode.jsonc`,
        `${process.env.HOME}/.config/opencode/opencode.json`,
      ]
      for (const cfg of configs) {
        if (!(await Bun.file(cfg).exists())) continue
        const d = JSON.parse(stripJsonc(await Bun.file(cfg).text())) as {
          mcp?: Record<string, { headers?: Record<string, string> }>
        }
        const auth = d.mcp?.agentbus?.headers?.Authorization ?? ""
        const m = auth.match(/^\s*[Bb]earer\s+(\S+)\s*$/)
        if (m) {
          // Name WHICH config supplied the credential, so a mis-resolved agent
          // is diagnosed instead of guessed at. The project path is the one
          // setup opencode writes, and the name is what tells an operator that
          // this session is being served by its own declared identity.
          await mark(`credential from ${cfg}`)
          return m[1]
        }
      }
      return ""
    } catch { return "" }
  }

  /** Ask the credential who it is. NEVER guess, and never fall back. */
  async function resolveAgent(key: string): Promise<string> {
    if (!key) {
      // Name BOTH files, because the lookup reads both. Saying only opencode.json
      // sent bob looking in a file his host does not have — his config is .jsonc,
      // which is why the .jsonc-first lookup exists twenty lines up. A diagnostic
      // that names the wrong file is worse than none: it spends the reader's time
      // proving the message wrong before they can start on the actual problem.
      await mark("no credential: no AGENTBUS_API_KEY, and no bearer token in " +
        "mcp.agentbus in opencode.jsonc or opencode.json")
      return ""
    }
    const res = await runWithKey(["--json", "whoami"], key)
    if (res.code !== 0) {
      // Record the REASON. A wake that never starts and a wake that starts and
      // hears nothing look the same from the session, so the failure has to be
      // written down at the moment it happens.
      await mark(`whoami failed (exit ${res.code}): ${(res.stdout + res.stderr).trim().slice(0, 200)}`)
      return ""
    }
    try {
      const d = JSON.parse(res.stdout) as {
        agent?: { name?: string }; key?: { bound_agents?: string[] }
      }
      return d.agent?.name || d.key?.bound_agents?.[0] || ""
    } catch {
      await mark(`whoami returned unparseable output: ${res.stdout.slice(0, 200)}`)
      return ""
    }
  }

  /** Run the CLI with an explicit credential rather than the host default. */
  async function runWithKey(args: string[], key: string): Promise<HookResult> {
    try {
      const out = await $`${CLI_BIN} ${args}`.env({ ...process.env, AGENTBUS_API_KEY: key })
        .quiet().nothrow()
      return {
        code: out.exitCode ?? 0,
        stdout: out.stdout?.toString() ?? "",
        stderr: out.stderr?.toString() ?? "",
      }
    } catch (err) {
      return { code: 127, stdout: "", stderr: String(err) }
    }
  }

  /** Credentials for every shared-client invocation, empty when unresolved. */
  function hookEnv(): Record<string, string> | undefined {
    if (!sessionKey) return undefined
    const e: Record<string, string> = { AGENTBUS_API_KEY: sessionKey }
    if (agentName) e.AGENTBUS_AGENT = agentName
    return e
  }

  /** Surface text in the session, degrading through the available surfaces. */
  async function tell(sessionID: string, text: string): Promise<boolean> {
    // EVERY FAILURE IS RECORDED. A swallowed exception here is indistinguishable
    // from a delivered wake: the arrival gets logged, nothing appears on screen,
    // and the plugin looks like it worked. That happened — bob's ping was
    // received and announced to nobody, because both surfaces threw into empty
    // catch blocks.
    if (sessionID) {
      try {
        await client.session.promptAsync({
          path: { id: sessionID },
          body: { parts: [{ type: "text", text }] },
        } as never)
        await mark(`tell: promptAsync OK (session ${sessionID})`)
        return true
      } catch (e) {
        await mark(`tell: promptAsync FAILED (session ${sessionID}): ${String(e).slice(0, 300)}`)
      }
    } else {
      await mark("tell: no session id known, using TUI surfaces")
    }
    // APPEND THEN SUBMIT. Append alone only types into the input box; it does
    // not send. A wake that leaves text sitting unsent is a notification, not a
    // wake — the agent still does nothing until a human presses enter.
    try {
      await client.tui.appendPrompt({ body: { text } } as never)
      await mark("tell: appendPrompt OK")
    } catch (e) {
      await mark(`tell: appendPrompt FAILED: ${String(e).slice(0, 300)}`)
      return false
    }
    try {
      await client.tui.submitPrompt({} as never)
      await mark("tell: submitPrompt OK")
      return true
    } catch (e) {
      // Appended but not sent: the text is sitting in the input box unsent.
      // That is a notification, not a wake, so report it as NOT delivered and
      // let the caller buffer it for a session that can actually act.
      await mark(`tell: submitPrompt FAILED: ${String(e).slice(0, 300)}`)
      return false
    }
  }

  /**
   * Deliver one arrival. Works with or without a session, because the whole
   * point is to reach an agent that is NOT currently in a turn.
   */
  async function deliver(text: string, key: string): Promise<void> {
    if (key) {
      if (announced.has(key)) return
      announced.add(key)
    }
    const now = Date.now()
    if (!budgetAllows(now)) {
      await mark(`SUPPRESSED (injection budget): ${text.slice(0, 80)}`)
      return
    }
    if (currentSession) { await tell(currentSession, text); return }
    // No session yet. submitPrompt SENDS the text, so if that succeeds the
    // agent is already acting and buffering the same text would deliver it
    // again — which is what happened: one arrival reached the agent three
    // times, and the agent itself reported "delivered twice (duplicate)".
    //
    // Buffer ONLY when the TUI path could not deliver. Dropping it in that case
    // would recreate the silent inbox one layer up; duplicating it when
    // delivery worked is noise the agent has to reason about.
    const delivered = await tell("", text)
    if (!delivered) pending.push(text)
  }

  /**
   * START THE WAKE: read this agent's SSE stream directly and turn each new
   * delivery into a turn.
   *
   * DIRECT fetch, NOT a `agentbus watch` subprocess writing a file we poll.
   * The earlier design spawned the CLI with `--append` and tailed the file
   * every 1.5s, which bought three problems for nothing:
   *
   *   - every `watch` for an agent shares ONE cursor file
   *     (watch-<workspace>-<agent>.json), so two sessions of the same agent
   *     RACE and split messages between them instead of each being woken;
   *   - a subprocess to supervise, kill and misdiagnose;
   *   - polling latency on top of a push transport.
   *
   * There is no JS SDK to reuse — `sdk/` is Python, and this runs in Bun — but
   * /v1/stream is plain SSE, so fetch is the whole client. Policy still lives
   * in ONE place: the gate and the greeting continue to shell out to
   * `agentbus-hook`. Only the transport is local.
   *
   * SKIPPING HISTORY IS EXACT, NOT HEURISTIC. The server replays the backlog,
   * then emits a literal `: connected` comment, then live events. Everything
   * before that marker is history and must NOT wake anyone — waking on it means
   * acting on messages from hours ago. Everything after it is a real arrival.
   */
  // Server keepalive is 20s (api/routes_messages.py). Three missed beats before
  // we call the link dead: a healthy quiet stream must never trip this, or the
  // client reconnect-storms — the same defect with the opposite sign.
  const STREAM_KEEPALIVE_MS = 20_000
  const STREAM_IDLE_DEADLINE_MS = 3 * STREAM_KEEPALIVE_MS

  // A KEEPALIVE IS BYTES, SO THE IDLE DEADLINE CAN NEVER FIRE ON A LIVE SERVER.
  //
  // bob reproduced this against a real opencode session, control proven first: a
  // stub sending ONLY `: keepalive` frames never tripped STREAM_IDLE_DEADLINE_MS,
  // because every keepalive settles reader.read() and resets the deadline. The
  // deadline therefore catches a link that goes SILENT and catches nothing at all
  // on a server that is keepaliving — which is every healthy server. If a push is
  // ever missed to this stream, this client has no second way to find out and the
  // agent is deaf until the process restarts.
  //
  // Identical to the Python defect fixed in 0.4.39 (STREAM_RECONCILE_SECONDS).
  // It shipped there and not here: same defect class, one client fixed, the other
  // not. That asymmetry is the finding, and it is why parity is checked now.
  //
  // The reconcile is a CHEAP inbox check, NOT a periodic reconnect. An idle agent
  // must not reconnect every minute — that trades a silent failure for permanent
  // load, which is the reconnect-storm failure three lines up wearing a different
  // hat. Only when the server says something exists after our last id do we drop
  // the stream, and the reconnect replays it through the delivery path that is
  // already proven.
  const STREAM_RECONCILE_MS = 60_000

  /** Reject if `p` has not settled within the idle deadline. */
  function withIdleDeadline<T>(p: Promise<T>, onExpire: () => void): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        onExpire()
        reject(new Error(
          `no data for ${STREAM_IDLE_DEADLINE_MS / 1000}s though the server sends a ` +
          `keepalive every ${STREAM_KEEPALIVE_MS / 1000}s — treating the link as dead`))
      }, STREAM_IDLE_DEADLINE_MS)
      p.then(
        (v) => { clearTimeout(timer); resolve(v) },
        (e) => { clearTimeout(timer); reject(e) },
      )
    })
  }

  async function startWake(): Promise<void> {
    if (watcher) return
    sessionKey = await resolveKey()
    const key = sessionKey
    agentName = await resolveAgent(key)
    if (!agentName) {
      await mark("wake NOT started: no identity could be established for this session")
      return
    }

    const base = (process.env.AGENTBUS_BASE_URL || "https://agentbus.rodmena.co.uk")
      .replace(/\/+$/, "")
    const ac = new AbortController()
    watcher = { kill: () => ac.abort() }

    // Reconnect forever with backoff. A wake path that gives up after N tries
    // goes quiet exactly like a working one with an empty inbox.
    void (async () => {
      let lastId = ""

      /**
       * Did the server accept mail this stream never pushed us?
       *
       * One inbox GET for a single row after `lastId`. It deliberately does NOT
       * deliver anything: waking from two code paths is how a message arrives
       * twice, and the reconnect below already replays correctly from
       * Last-Event-ID. This answers one question and takes no action.
       *
       * Any failure answers NO. Reconnecting on a network blip would turn a
       * transient error into a reconnect loop, and a genuinely dead link is still
       * covered by the idle deadline.
       */
      async function streamMissedMail(cursor: string): Promise<boolean> {
        try {
          const q = `cursor=${encodeURIComponent(cursor || "0")}&limit=1`
          const r = await fetch(`${base}/v1/inbox?${q}`, {
            headers: { Authorization: `Bearer ${key}`, "X-AgentBus-Agent": agentName },
          })
          if (!r.ok) return false
          const body = await r.json() as { messages?: unknown[] }
          return Array.isArray(body.messages) && body.messages.length > 0
        } catch { return false }
      }
      let backoff = 1000
      for (;;) {
        if (ac.signal.aborted) return
        let live = false          // have we passed the `: connected` marker?
        try {
          const headers: Record<string, string> = {
            Authorization: `Bearer ${key}`,
            "X-AgentBus-Agent": agentName,
            Accept: "text/event-stream",
          }
          // On a RESUME we do want what was missed, so the cursor is sent and
          // everything after it counts as live. Only the FIRST connection of a
          // process discards history.
          if (lastId) { headers["Last-Event-ID"] = lastId; live = true }

          const res = await fetch(`${base}/v1/stream`, { headers, signal: ac.signal })
          if (!res.ok || !res.body) {
            await mark(`stream connect failed: HTTP ${res.status}`)
            throw new Error(`HTTP ${res.status}`)
          }
          await mark(`stream connected as ${agentName}${lastId ? ` (resume ${lastId})` : " (live only)"}`)
          backoff = 1000

          const reader = res.body.getReader()
          const decoder = new TextDecoder()
          let buf = ""
          // Per connection: a fresh stream has just replayed its backlog, so the
          // clock starts now instead of firing a reconcile immediately.
          let lastReconcile = Date.now()
          for (;;) {
            // IDLE DEADLINE — the only way this client can notice a link that
            // died WITHOUT a close.
            //
            // A clean disconnect resolves read() with done, or rejects, and the
            // backoff below runs. But when the far end simply stops (box moved,
            // blackholed route, dropped NAT state — what a database migration
            // produces) nothing arrives and nothing settles: read() pends
            // forever on a socket that still looks established, and this agent
            // is deaf until the process restarts. Verified in the Python client
            // twice against a real service; this loop had the identical hole.
            //
            // The server sends `: keepalive` every 20s on an idle stream, so
            // silence IS diagnostic — there was simply no deadline by which to
            // notice it. 60s = three missed keepalives: long enough that a
            // healthy quiet link never trips it, finite so a dead one always
            // does.
            const { done, value } = await withIdleDeadline(reader.read(), () => {
              // Free the socket before the reconnect, or each outage leaks one.
              void reader.cancel().catch(() => {})
            })
            if (done) break

            // THE RECONCILE TICK. Runs on ANY traffic, keepalives included, which
            // is the whole point: keepalives are exactly what stops the idle
            // deadline from ever firing, so they are the only clock this client
            // reliably has.
            if (Date.now() - lastReconcile >= STREAM_RECONCILE_MS) {
              lastReconcile = Date.now()
              if (await streamMissedMail(lastId)) {
                await mark(
                  "stream: reconcile found mail the stream never pushed — " +
                  "reconnecting to replay it")
                void reader.cancel().catch(() => {})
                break
              }
            }

            buf += decoder.decode(value, { stream: true })
            // SSE frames are separated by a blank line.
            let cut: number
            while ((cut = buf.indexOf("\n\n")) !== -1) {
              const frame = buf.slice(0, cut)
              buf = buf.slice(cut + 2)
              if (frame.startsWith(":")) {
                // `: connected` closes the backlog. Everything after is live.
                if (frame.includes("connected")) {
                  live = true
                  await mark("stream: backlog ended, now live")
                }
                continue
              }
              let id = "", ev = "", data = ""
              for (const line of frame.split("\n")) {
                if (line.startsWith("id:")) id = line.slice(3).trim()
                else if (line.startsWith("event:")) ev = line.slice(6).trim()
                else if (line.startsWith("data:")) data += line.slice(5).trim()
              }
              if (id) lastId = id
              if (ev === "unauthorized") {
                await mark("stream: UNAUTHORIZED — credential revoked; wake path is down")
                return
              }
              // ONLY DELIVERIES WAKE. Without this filter every event on the
              // agent's channel — acks, presence, agent lifecycle — became a
              // wake, which is how "(no subject)" reached the agent as if a peer
              // had written it.
              if (!live || !data || ev !== "message.delivered") continue
              let m: {
                subject?: string; sender_display?: string; sender_address?: string
                delivery_id?: string
              }
              try { m = JSON.parse(data) } catch { continue }
              // The LIVE event carries type/delivery_id/message_id/seq/subject
              // and no sender; the replayed backlog frames carry the full row.
              // So the sender clause is written only when a sender is actually
              // known — naming "a peer" as the sender is inventing a fact, and
              // the agent quotes what it is told.
              const from = m.sender_display || m.sender_address || ""
              const subj = m.subject || "(no subject)"
              await mark(`arrival: ${from || "(sender not in event)"} — ${subj}`)
              // #146 parity with the claude plugin: the notice names BOTH verbs.
              // "Read it" alone taught agents half the loop — they read peer
              // mail and never answered it, or answered by some other route.
              await deliver(
                `AgentBus: new message${from ? ` from ${from}` : ""} — "${subj}". ` +
                `Read it in full with \`agentbus show ${m.delivery_id ?? ""}\`, ` +
                `print the complete body, then act on it. ` +
                `Reply in-thread with \`agentbus reply ${m.delivery_id ?? "<id>"} -b '...'\`.`,
                m.delivery_id ?? `${from}:${subj}`)
            }
          }
          await mark("stream ended; reconnecting")
        } catch (e) {
          if (ac.signal.aborted) return
          await mark(`stream error: ${String(e).slice(0, 200)}`)
        }
        await new Promise((r) => setTimeout(r, backoff))
        backoff = Math.min(backoff * 2, 30_000)
      }
    })()
  }

  // START LISTENING NOW — not on the first session, not on the first prompt.
  // Awaited so a failure is recorded in the marker before any hook can run and
  // make the plugin look alive.
  await startWake()

  return {
    /**
     * SESSION START. opencode has no dedicated hook, so the first sighting of a
     * session id is used — deduplicated, because `event` fires repeatedly and
     * re-announcing the same backlog every tick trains the reader to ignore it.
     */
    event: async ({ event }) => {
      const id = (event as { properties?: { info?: { id?: string } } })
        ?.properties?.info?.id

      // `event` FIRES FOR MESSAGES TOO, AND info.id IS THEN A MESSAGE ID.
      //
      // Treating that as a session id was a loop: every message the plugin
      // injected produced a message event whose msg_… id had never been seen,
      // so it looked like a brand-new session and re-ran the session-start
      // greeting — which injected another message. One arrival became nine
      // injections and still climbing when I killed it. Measured: 7 of the 9
      // were addressed to msg_… ids, 2 to a real ses_… id.
      //
      // Only a session id names a session. Anything else is not ours to answer.
      if (!id || !id.startsWith("ses_")) return
      if (seenSessions.has(id)) return
      seenSessions.add(id)

      const sessionID = id
      currentSession = sessionID
      // Bounded: a 195-message backlog dumped verbatim is not a greeting, it is
      // a wall the reader scrolls past. Once per PROCESS — bounding it per
      // session is no protection against a bug that invents session ids — and
      // through the same budget as every other injection.
      const res = await run($, HOOK_BIN, ["session-start"], undefined, hookEnv())
      const greeting = res.stdout.trim()
      if (greeting && !greeted && budgetAllows(Date.now())) {
        greeted = true
        await tell(sessionID, greeting.length > 1200
          ? greeting.slice(0, 1200) + "\n… truncated; run `agentbus inbox --unread` for the rest."
          : greeting)
      }
      // Flush anything that arrived while there was no session to act in.
      while (pending.length) await tell(sessionID, pending.shift() as string)
    },

    /** Per-turn catch-up, the UserPromptSubmit equivalent. */
    /**
     * PER-TURN HOOK — AND IT MUST NOT INJECT. THIS IS WHERE THE LOOP WAS.
     *
     * It called `agentbus-hook pending`, which re-announces the WHOLE unread
     * backlog (195 messages here), and injected the result. But an injection is
     * itself a chat message, so it fired this hook again, which announced the
     * backlog again, forever. One real arrival became thousands of messages and
     * overloaded the client.
     *
     * The rule that falls out: NOTHING THE PLUGIN INJECTS MAY TRIGGER ANOTHER
     * INJECTION. Arrivals come from the watch stream, which is driven by the
     * bus and cannot be driven by us. This hook now only records where to
     * deliver, and flushes what the stream already queued.
     */
    "chat.message": async ({ sessionID }) => {
      if (!sessionID) return
      currentSession = sessionID
      while (pending.length) await tell(sessionID, pending.shift() as string)
    },

    /**
     * THE GATE, verdict form. FAILS CLOSED: anything other than an explicit
     * allow denies. A gate that permits when it cannot reach its rules is worse
     * than no gate, because it reports as protection.
     *
     * OPT-IN GATE (same rule as tool.execute.before): no credential resolved for
     * THIS session means this session is not AgentBus, and must not be failed
     * closed by host-global state. Only a session with its own credential is
     * consulted.
     */
    "permission.ask": async (permission, output) => {
      const env = hookEnv()
      if (!env) return
      const payload = JSON.stringify({
        tool_name: (permission as { type?: string }).type ?? "unknown",
        tool_input: permission,
      })
      const res = await run($, HOOK_BIN, ["pre-tool-use"], payload, env)
      let allow = false
      try {
        const parsed = JSON.parse(res.stdout) as {
          hookSpecificOutput?: { permissionDecision?: string }
        }
        allow = parsed.hookSpecificOutput?.permissionDecision === "allow"
      } catch { allow = false }
      if (!allow) output.status = "deny"
    },

    /**
     * THE GATE, enforcement form. Fires for EVERY tool call, including ones
     * opencode never asks about — the population `permission.ask` cannot see.
     * Enforces by throwing, because this hook's output carries only `args`.
     *
     * THE OPT-IN GATE. A session is AgentBus-gated ONLY when it has its own
     * credential (`sessionKey`). The credential cannot come from the inherited
     * process env: opencode sessions in ANY project on a machine that also
     * runs AgentBus would inherit `AGENTBUS_AGENT`/`AGENTBUS_API_KEY` from the
     * shell, and every such session — including unrelated ones, e.g. in
     * another app entirely — would be failed-closed by `pre-tool-use`. That
     * is how a prism session that never opted into AgentBus got every tool
     * blocked (2026-08-10): the gate derived "is this an AgentBus session"
     * from host-global state instead of this session's own credential.
     */
    "tool.execute.before": async (info, _output) => {
      const env = hookEnv()
      if (!env) return  // no credential for THIS session: not AgentBus, ungated
      const payload = JSON.stringify({ tool_name: info.tool, tool_input: _output })
      const res = await run($, HOOK_BIN, ["pre-tool-use"], payload, env)
      let decision = ""
      let reason = ""
      try {
        const parsed = JSON.parse(res.stdout) as {
          hookSpecificOutput?: { permissionDecision?: string; permissionDecisionReason?: string }
        }
        decision = parsed.hookSpecificOutput?.permissionDecision ?? ""
        reason = parsed.hookSpecificOutput?.permissionDecisionReason ?? ""
      } catch {
        // Unparseable is undecidable, and undecidable denies — UNLESS the client
        // exited 0, which is how it reports "no credential on this host, and so
        // nothing to enforce". Denying every tool call on a machine that never
        // opted in is an outage dressed as security.
        if (res.code !== 0) {
          throw new Error(
            "AgentBus could not confirm this action is permitted " +
            `(hook exit ${res.code}). Blocked because approval could not be ` +
            "checked, not because it was refused.",
          )
        }
        return
      }
      if (decision === "deny") throw new Error(`AgentBus blocked this action: ${reason}`)
    },

    /** SessionEnd equivalent — stop the wake rather than leaving it orphaned. */
    dispose: async () => {
      try { watcher?.kill() } catch { /* already exited is the outcome we wanted */ }
      await mark("disposed")
      await run($, HOOK_BIN, ["session-end"], undefined, hookEnv())
    },
  }
}

export default server
