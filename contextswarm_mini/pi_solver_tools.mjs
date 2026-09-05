// Controlled solver capabilities.  This extension deliberately exposes no
// arbitrary process or network primitive: every dynamic operation is forwarded
// to the runner-owned, token-bound loopback broker.
import { existsSync, lstatSync, realpathSync } from "node:fs";
import { isAbsolute, relative, resolve, sep } from "node:path";

const objectSchema = (properties, required = []) => ({
  type: "object",
  properties,
  required,
  additionalProperties: false,
});

const stringSchema = (description, maxLength) => ({ type: "string", description, maxLength });
const integerSchema = (description, maximum = 8, minimum = 1) => {
  const schema = {
    type: "integer",
    description,
    minimum,
  };
  if (maximum !== undefined && maximum !== null) schema.maximum = maximum;
  return schema;
};

function brokerBaseUrl() {
  const raw = String(process.env.CONTEXTSWARM_JUDGE_URL ?? "").trim();
  if (!raw) throw new Error("The controlled experiment broker is unavailable.");
  let parsed;
  try {
    parsed = new URL(raw);
  } catch {
    throw new Error("The controlled experiment broker configuration is invalid.");
  }
  if (
    parsed.protocol !== "http:" ||
    !["127.0.0.1", "localhost", "[::1]"].includes(parsed.hostname) ||
    parsed.username ||
    parsed.password ||
    parsed.search ||
    parsed.hash
  ) {
    throw new Error("The controlled experiment broker configuration is invalid.");
  }
  return raw.replace(/\/+$/, "");
}

async function brokerCall(operation, payload, signal) {
  const controller = new AbortController();
  const rawDeadline = Number(process.env.CONTEXTSWARM_BROKER_DEADLINE_EPOCH_MS ?? "");
  const timeoutMs = Number.isFinite(rawDeadline) && rawDeadline > 0
    ? Math.min(2_147_000_000, Math.max(1_000, rawDeadline - Date.now() + 10_000))
    : null;
  // The broker and evaluator stop at the runner-owned absolute deadline.  The
  // client grace prevents a fixed local timer from cancelling a legitimate
  // gate wait or Judge Retry-After first; Pi's parent signal remains the final
  // cancellation authority when no broker deadline is present.
  const timeout = timeoutMs === null ? null : setTimeout(() => controller.abort(), timeoutMs);
  const abortFromParent = () => controller.abort();
  signal?.addEventListener("abort", abortFromParent, { once: true });
  try {
    const response = await fetch(`${brokerBaseUrl()}/${operation}`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify(payload ?? {}),
      signal: controller.signal,
    });
    if (!response.ok) {
      throw new Error("The controlled experiment broker rejected this capability call.");
    }
    const result = await response.json();
    if (!result || typeof result !== "object" || Array.isArray(result)) {
      throw new Error("The controlled experiment broker returned an invalid response.");
    }
    return result;
  } catch (error) {
    if (controller.signal.aborted) {
      throw new Error("The controlled experiment broker call was cancelled or timed out.");
    }
    if (error instanceof Error && error.message.startsWith("The controlled")) throw error;
    throw new Error("The controlled experiment broker call failed.");
  } finally {
    if (timeout !== null) clearTimeout(timeout);
    signal?.removeEventListener("abort", abortFromParent);
  }
}

function toolResult(payload) {
  let text = JSON.stringify(payload, null, 2);
  if (text.length > 64_000) {
    text = `${text.slice(0, 64_000)}\n[controlled tool output truncated]`;
  }
  return {
    content: [{ type: "text", text }],
    details: {
      status: typeof payload?.status === "string" ? payload.status : undefined,
      ok: payload?.ok === true,
    },
  };
}

function registerBrokerTool(pi, definition) {
  pi.registerTool({
    ...definition,
    executionMode: "sequential",
    async execute(_toolCallId, params, signal) {
      return toolResult(await brokerCall(definition.name, params, signal));
    },
  });
}

function enabledCapability(name, defaultValue = false) {
  const raw = String(process.env[name] ?? "").trim().toLowerCase();
  if (!raw) return defaultValue;
  return ["1", "true", "yes", "on"].includes(raw);
}

function agentTimeoutBounds() {
  const rawMaximum = Number(process.env.CONTEXTSWARM_AGENT_TIMEOUT_MAX_SECONDS ?? "");
  const maximum = Number.isSafeInteger(rawMaximum) && rawMaximum > 0 ? rawMaximum : 300;
  const minimum = Math.min(5, maximum);
  return { minimum, maximum };
}

function scaledAgentTimeout(maximum, minimum, ratio) {
  return Math.max(minimum, Math.min(maximum, Math.round(maximum * ratio)));
}

function timeoutTier(low, high, ratioLabel) {
  const rendered = `${low}-${high}s`;
  return low === high
    ? `${rendered} (rounded for this configured cap)`
    : `${rendered} (${ratioLabel} of the cap)`;
}

function cpsScopeProperties(allowGlobal) {
  if (!allowGlobal) return {};
  return {
    scope: {
      type: "string",
      enum: ["task", "global"],
      description: "global is available only in hybrid mode",
    },
  };
}

function normalizeExistingPath(rawPath, cwd) {
  if (typeof rawPath !== "string" || !rawPath.trim()) return null;
  const lexical = isAbsolute(rawPath) ? resolve(rawPath) : resolve(cwd, rawPath);
  if (!existsSync(lexical)) return null;
  try {
    if (lstatSync(lexical).isSymbolicLink()) return null;
    return realpathSync(lexical);
  } catch {
    return null;
  }
}

function relativeInside(path, cwd) {
  const rel = relative(cwd, path);
  if (!rel || rel === ".") return ".";
  if (rel === ".." || rel.startsWith(`..${sep}`) || isAbsolute(rel)) return null;
  return rel.split(sep).join("/");
}

// The runner binds the candidate filename per worker session.  Keep the
// historical formal default and reject every other spelling so task data cannot
// widen this capability into an arbitrary path.
function candidateFilename() {
  const configured = String(process.env.CONTEXTSWARM_CANDIDATE_FILENAME ?? "").trim();
  return configured === "result.cpp" || configured === "result.lean"
    ? configured
    : "result.lean";
}

function candidateExtension() {
  return candidateFilename() === "result.cpp" ? "cpp" : "lean";
}

function escapedCandidateFilename() {
  return candidateFilename().replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function isReadableFile(rel) {
  const candidate = candidateFilename();
  const candidatePattern = escapedCandidateFilename();
  const extension = candidateExtension();
  return (
    ["problem.md", candidate, "metadata.json", "PUBLIC_FILES.md"].includes(rel) ||
    new RegExp(`^baseline/[^/]+\\.${extension}$`).test(rel) ||
    new RegExp(`^tasks/[^/]+/(?:problem\\.md|${candidatePattern}|metadata\\.json|PUBLIC_FILES\\.md)$`).test(rel) ||
    new RegExp(`^tasks/[^/]+/baseline/[^/]+\\.${extension}$`).test(rel)
  );
}

function isWritableCandidate(rel) {
  const candidate = candidateFilename();
  return rel === candidate || new RegExp(`^tasks/[^/]+/${escapedCandidateFilename()}$`).test(rel);
}

// Unlike read/search paths, the final candidate may not exist yet (the write
// tool is allowed to create result.lean).  Resolve the path lexically, but
// inspect every existing component with lstat before allowing a write/edit.
// Otherwise a pre-existing result.lean symlink (or a symlinked task directory)
// could make an apparently in-workspace write modify an arbitrary host file.
function writableRelative(rawPath, cwd) {
  if (typeof rawPath !== "string" || !rawPath.trim()) return null;
  const lexical = isAbsolute(rawPath) ? resolve(rawPath) : resolve(cwd, rawPath);
  const rel = relativeInside(lexical, cwd);
  if (!rel || !isWritableCandidate(rel)) return null;
  const parts = rel.split("/");
  let current = cwd;
  for (let index = 0; index < parts.length; index += 1) {
    current = resolve(current, parts[index]);
    try {
      if (lstatSync(current).isSymbolicLink()) return null;
    } catch (error) {
      // A missing final result.lean is valid, but a missing parent directory
      // (or any non-ENOENT lookup failure) must not be accepted because the
      // tool could otherwise operate on an unexpected path after the error is
      // resolved.  In particular, ENOTDIR at the final component means a
      // parent is a regular file, not that result.lean is safely absent.
      if (index !== parts.length - 1 || error?.code !== "ENOENT") return null;
    }
  }
  return rel;
}

function isSafeSearchDirectory(rel) {
  return (
    rel === "baseline" ||
    rel === "tasks" ||
    /^tasks\/[^/]+$/.test(rel) ||
    /^tasks\/[^/]+\/baseline$/.test(rel)
  );
}

function guardedRelative(rawPath, ctx) {
  const configured = String(process.env.CONTEXTSWARM_WORKDIR ?? "").trim();
  let cwd;
  try {
    cwd = realpathSync(configured || ctx.cwd);
  } catch {
    return null;
  }
  const target = normalizeExistingPath(rawPath, cwd);
  return target ? relativeInside(target, cwd) : null;
}

function boundedShellTokens(command) {
  if (typeof command !== "string" || command.length < 1 || command.length > 16_000) return null;
  const tokens = [];
  let token = "";
  let quote = null;
  let tokenStarted = false;
  for (let index = 0; index < command.length; index += 1) {
    const char = command[index];
    if (char === "\0" || char === "\n" || char === "\r") return null;
    if (quote === "'") {
      if (char === "'") quote = null;
      else token += char;
      tokenStarted = true;
      continue;
    }
    if (quote === '"') {
      if (char === '"') {
        quote = null;
      } else {
        // Double-quoted shell text still performs substitutions and escapes.
        if (char === "$" || char === "`" || char === "\\") return null;
        token += char;
      }
      tokenStarted = true;
      continue;
    }
    if (char === "'" || char === '"') {
      quote = char;
      tokenStarted = true;
      continue;
    }
    if (/\s/.test(char)) {
      if (tokenStarted) tokens.push(token);
      token = "";
      tokenStarted = false;
      continue;
    }
    // Reject every shell control, substitution, redirection, globbing, and
    // expansion character. Quoted Lean snippets remain ordinary argv text.
    if (/[;&|<>`$\\*?\[\](){}#]/.test(char)) return null;
    token += char;
    tokenStarted = true;
  }
  if (quote !== null) return null;
  if (tokenStarted) tokens.push(token);
  if (tokens.length < 1 || tokens.length > 80 || tokens.some((value) => value.length > 8_000)) {
    return null;
  }
  return tokens;
}

function formalHelperRelative(rawTarget, ctx) {
  const configured = String(process.env.CONTEXTSWARM_WORKDIR ?? "").trim();
  let cwd;
  try {
    cwd = realpathSync(configured || ctx.cwd);
  } catch {
    return null;
  }
  const target = normalizeExistingPath(rawTarget, cwd);
  return target ? relativeInside(target, cwd) : null;
}

function isAllowedFormalCommand(command, ctx) {
  // Coding workers never receive the formal helper capability, and must not
  // be able to reach it even if a tool-call event is forged locally.
  if (candidateFilename() !== "result.lean") return false;
  const tokens = boundedShellTokens(command);
  if (!tokens) return false;
  const mode = String(process.env.CONTEXTSWARM_EXPERIMENT_MODE ?? "").trim().toLowerCase();
  if (tokens[0] === "python3") {
    // ``python3`` is intentionally a short spelling in the public helper
    // contract, so bind its resolution to the supervisor's fixed PATH.  A
    // worker-controlled PATH (or a same-named executable in the workspace)
    // must not turn this into arbitrary code execution.
    if (process.env.PATH !== "/usr/local/bin:/usr/bin:/bin") return false;
    const timeoutEnabled = enabledCapability("CONTEXTSWARM_AGENT_TIMEOUT_ENABLED");
    const helperInvocation =
      tokens.length === 2 ||
      (timeoutEnabled &&
        tokens.length === 4 &&
        tokens[2] === "--timeout" &&
        /^[0-9]+$/.test(tokens[3]));
    if (!helperInvocation) return false;
    const rel = formalHelperRelative(tokens[1], ctx);
    return rel === "evaluate.py" || (mode === "mono" && /^tasks\/[^/]+\/evaluate\.py$/.test(rel ?? ""));
  }
  // A slash is required so the shell executes the exact path we validated,
  // rather than resolving a same-named executable from PATH afterward.
  if (!tokens[0].includes("/")) return false;
  const rel = formalHelperRelative(tokens[0], ctx);
  return rel === "formal_query" || (mode === "mono" && /^tasks\/[^/]+\/formal_query$/.test(rel ?? ""));
}

function installPathGuard(pi) {
  pi.on("tool_call", (event, ctx) => {
    const input = event?.input && typeof event.input === "object" ? event.input : {};
    if (event.toolName === "read") {
      const rel = guardedRelative(input.path, ctx);
      if (!rel || !isReadableFile(rel)) {
        return { block: true, reason: "read is restricted to assigned public task files" };
      }
      return;
    }
    if (event.toolName === "write" || event.toolName === "edit") {
      const configured = String(process.env.CONTEXTSWARM_WORKDIR ?? "").trim();
      let cwd;
      try {
        cwd = realpathSync(configured || ctx.cwd);
      } catch {
        return { block: true, reason: "assigned workspace is unavailable" };
      }
      const rel = writableRelative(input.path, cwd);
      if (!rel) {
        return {
          block: true,
          reason: `write/edit is restricted to assigned ${candidateFilename()}`,
        };
      }
      return;
    }
    if (event.toolName === "grep") {
      const rel = guardedRelative(input.path ?? "", ctx);
      const safeGlob =
        input.glob === undefined ||
        (typeof input.glob === "string" && !/[\\/]/.test(input.glob) && !input.glob.includes(".."));
      if (!rel || (!isReadableFile(rel) && !isSafeSearchDirectory(rel)) || !safeGlob) {
        return { block: true, reason: "grep requires an explicit assigned task file or safe task directory" };
      }
      return;
    }
    if (event.toolName === "find") {
      const rel = guardedRelative(input.path ?? "", ctx);
      const safePattern =
        typeof input.pattern === "string" &&
        !/[\\/]/.test(input.pattern) &&
        !input.pattern.includes("..");
      if (!rel || !isSafeSearchDirectory(rel) || !safePattern) {
        return { block: true, reason: "find is restricted to safe assigned task directories" };
      }
      return;
    }
    if (event.toolName === "ls") {
      const rel = guardedRelative(input.path ?? "", ctx);
      if (!rel || !isSafeSearchDirectory(rel)) {
        return { block: true, reason: "ls is restricted to safe assigned task directories" };
      }
      return;
    }
    if (event.toolName === "bash") {
      const command = typeof input.command === "string" ? input.command : input.cmd;
      if (!isAllowedFormalCommand(command, ctx)) {
        return {
          block: true,
          reason: "bash is restricted to the staged formal helper commands",
        };
      }
      const configuredTimeout = Number(
        process.env.CONTEXTSWARM_FORMAL_COMMAND_TIMEOUT_SECONDS ?? "420",
      );
      input.timeout = Number.isFinite(configuredTimeout)
        // Keep the shell guard finite while allowing a manifest-selected
        // Agent/Judge cap above the historical one-hour default.  The outer
        // experiment horizon and broker deadline remain independent hard
        // boundaries.
        ? Math.max(1, Math.min(2_147_000_000, Math.trunc(configuredTimeout)))
        : 420;
    }
  });
}

export default function registerContextSwarmSolverTools(pi) {
  installPathGuard(pi);

  const candidate = candidateFilename();
  const language = candidate === "result.cpp" ? "C++" : "Lean";
  // Direct messaging was part of the original CPS contract, so its default
  // remains enabled.  The runner sets these explicit, non-secret capability
  // bits for allocation/selection experiments that must remain message-free.
  const directMessages = enabledCapability("CONTEXTSWARM_CPS_DIRECT_MESSAGES", true);
  const selectionEnabled = enabledCapability("CONTEXTSWARM_CPS_SELECTION_ENABLED");
  const agentTimeoutEnabled = enabledCapability("CONTEXTSWARM_AGENT_TIMEOUT_ENABLED");
  const agentTimeout = agentTimeoutBounds();
  const routineTimeout = [
    scaledAgentTimeout(agentTimeout.maximum, agentTimeout.minimum, 0.10),
    scaledAgentTimeout(agentTimeout.maximum, agentTimeout.minimum, 0.20),
  ];
  const heavyTimeout = [
    scaledAgentTimeout(agentTimeout.maximum, agentTimeout.minimum, 0.40),
    scaledAgentTimeout(agentTimeout.maximum, agentTimeout.minimum, 0.60),
  ];
  const sanityTimeout = [
    agentTimeout.minimum,
    scaledAgentTimeout(agentTimeout.maximum, agentTimeout.minimum, 0.05),
  ];
  const routineGuidance = timeoutTier(routineTimeout[0], routineTimeout[1], "10-20%");
  const heavyGuidance = timeoutTier(heavyTimeout[0], heavyTimeout[1], "40-60%");
  const sanityGuidance = timeoutTier(sanityTimeout[0], sanityTimeout[1], "about 5% or less");

  const judgeProperties = {
    task_id: stringSchema("Mono task slug; omit in a single-task worker", 256),
  };
  if (agentTimeoutEnabled) {
    judgeProperties.timeout_seconds = integerSchema(
      `Optional cumulative validation budget in seconds across evaluator retries; runner clamps to ${agentTimeout.minimum}-${agentTimeout.maximum}`,
      null,
      agentTimeout.minimum,
    );
  }
  const globalScope = !selectionEnabled && enabledCapability("CONTEXTSWARM_CPS_GLOBAL_SCOPE");
  const scopeProperties = cpsScopeProperties(globalScope);

  registerBrokerTool(pi, {
    name: "judge_check",
    label: "Controlled Judge Check",
    description:
      `Submit the runner-bound ${candidate} to the controlled external ${language} Judge. The task, baseline, environment, profile, endpoint, deadline, and concurrency are fixed by the runner. For a normal single-task worker call with no arguments; Mono must provide task_id.${agentTimeoutEnabled ? ` You may optionally provide integer timeout_seconds (${agentTimeout.minimum}-${agentTimeout.maximum}) as the total budget for this logical validation, including safe retries; the runner clamps it and reports the effective budget.` : ""}`,
    promptSnippet: `Check the current ${candidate} through the controlled external Judge`,
    promptGuidelines: [
      "Use judge_check one candidate at a time; never attempt local compilation or raw Judge access.",
      "A retryable busy result is not permission to use a local fallback.",
      ...(agentTimeoutEnabled
        ? [
            `Choose timeout_seconds as a cumulative logical validation budget: about ${routineGuidance} for routine checks, ${heavyGuidance} for promising heavy candidates, and ${agentTimeout.maximum}s only for likely but known-slow checks; use about ${sanityGuidance} for cheap sanity feedback. Any safe retry receives only the remaining time.`,
            "An execution timeout is inconclusive feedback; do not relabel it as VERIFY_FAIL or use a local checker.",
          ]
        : []),
    ],
    parameters: objectSchema(judgeProperties),
  });

  registerBrokerTool(pi, {
    name: "cps_search",
    label: "Search Context Pieces",
    description: "Search bounded shared context for this runner-bound task.",
    promptSnippet: "Search shared CPS evidence for the assigned task",
    parameters: objectSchema({
      query: stringSchema("Search terms", 500),
      limit: integerSchema("Maximum returned pieces", 8),
    }),
  });

  registerBrokerTool(pi, {
    name: "cps_publish",
    label: "Publish Context Piece",
    description: "Publish a concise typed handoff to runner-owned CPS state.",
    promptSnippet: "Publish a concise CPS proof handoff",
    parameters: objectSchema(
      {
        kind: stringSchema("Piece type, such as proof_strategy, lemma, blocker, or handoff", 64),
        title: stringSchema("Concise title", 300),
        body: stringSchema("Reusable proof information", 8_000),
        tags: { type: "array", items: stringSchema("Tag", 64), maxItems: 8 },
        ...scopeProperties,
      },
      ["title", "body"],
    ),
  });

  if (directMessages) registerBrokerTool(pi, {
    name: "cps_inbox",
    label: "CPS Inbox",
    description: "Read bounded unacknowledged direct messages for this actor.",
    promptSnippet: "Read direct CPS messages for this actor",
    parameters: objectSchema({ limit: integerSchema("Maximum returned messages", 8) }),
  });

  if (directMessages) registerBrokerTool(pi, {
    name: "cps_send",
    label: "Send CPS Message",
    description: "Send a bounded direct message using the runner-bound actor identity.",
    promptSnippet: "Send a direct CPS handoff",
    parameters: objectSchema(
      {
        recipient: stringSchema("Recipient actor id; omit for a broadcast", 256),
        body: stringSchema("Message body", 8_000),
        ...scopeProperties,
      },
      ["body"],
    ),
  });

  if (selectionEnabled) registerBrokerTool(pi, {
    name: "cps_feedback",
    label: "Record CPS Exposure Feedback",
    description: "Record attributed feedback for one previously exposed ContextSwarm selection item. Supply the exposure identifiers exactly as returned by the selection surface.",
    promptSnippet: "Record attributed feedback for an exposed CPS selection item",
    parameters: objectSchema(
      {
        request_key: stringSchema("Idempotency key for this feedback event", 256),
        exposure_item_id: stringSchema("Identifier of the previously exposed selection item", 256),
        trace_id: stringSchema("Trace identifier returned with the exposed item", 256),
        feedback_kind: {
          type: "string",
          enum: ["useful", "not_useful", "misleading", "stale", "unsafe", "duplicate", "diagnostic_useful", "needs_refinement", "not_used", "route_attempted", "route_improving"],
          description: "Canonical attribution feedback kind",
        },
        value: { type: "number", description: "Optional numeric feedback value" },
        note: stringSchema("Optional concise attribution note", 8_000),
      },
      ["request_key", "exposure_item_id", "trace_id", "feedback_kind"],
    ),
  });

  if (directMessages) registerBrokerTool(pi, {
    name: "cps_ack",
    label: "Acknowledge CPS Message",
    description: "Acknowledge one message that is visible to this runner-bound actor.",
    promptSnippet: "Acknowledge a consumed CPS direct message",
    parameters: objectSchema(
      { message_id: stringSchema("Visible CPS message id", 64) },
      ["message_id"],
    ),
  });

  if (directMessages) registerBrokerTool(pi, {
    name: "cps_actors",
    label: "List CPS Actors",
    description: "Inspect the bounded public actor roster for recipient discovery.",
    promptSnippet: "Find a CPS actor for a direct handoff",
    parameters: objectSchema({ query: stringSchema("Optional actor/task filter", 300) }),
  });
}
