// @effect-diagnostics nodeBuiltinImport:off -- fs.watch and O_NOFOLLOW read the native session message queue.
import * as NodeFS from "node:fs";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";

import { ProviderDriverKind, type ProviderThreadId, type ThreadId } from "@t3tools/contracts";
import {
  backgroundWorkNotification,
  type BackgroundWorkReport,
} from "@t3tools/provider-core/server/notification";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import type * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import type * as EffectAcpSchema from "effect-acp/compat";

import { antigravityManagedPath, antigravityProfileRoot } from "./AntigravityProfilePaths.ts";

const ANTIGRAVITY_PROVIDER = ProviderDriverKind.make("antigravity");
const NATIVE_SESSION_ID = /^[\w-]+$/;
const MAX_MESSAGE_FILE_BYTES = 256 * 1024;
const BACKGROUND_TASK_OUTPUT = /Tool is running as a background task with task id:\s*([^\s"']+)/;
const COMMAND_EXIT_CODE = /Command exited with code (-?\d+)\./;
const TRAILING_OUTCOME_WORDS = /\s+(?:finished|completed|failed|stopped|cancelled)$/i;

const AntigravityToolCallRefSchema = Schema.Struct({
  id: Schema.optional(Schema.String),
  name: Schema.optional(Schema.String),
  argumentsJson: Schema.optional(Schema.String),
});

const AntigravityMessageSchema = Schema.Struct({
  id: Schema.String,
  recipient: Schema.optional(Schema.String),
  sender: Schema.optional(Schema.String),
  priority: Schema.optional(Schema.String),
  hideFromUser: Schema.optional(Schema.Boolean),
  content: Schema.optional(Schema.String),
  renderDetails: Schema.optional(
    Schema.Struct({
      messageTitle: Schema.optional(Schema.String),
    }),
  ),
  sourceMetadata: Schema.optional(
    Schema.Struct({
      tool: Schema.optional(
        Schema.Struct({
          toolCall: Schema.optional(AntigravityToolCallRefSchema),
        }),
      ),
    }),
  ),
});

type AntigravityMessage = typeof AntigravityMessageSchema.Type;

const decodeAntigravityMessage = Schema.decodeUnknownOption(
  Schema.fromJsonString(AntigravityMessageSchema),
);
const decodeReadMap = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Boolean)),
);
const decodeArgumentsJson = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.Unknown)),
);
const decodeRecord = Schema.decodeUnknownOption(Schema.Record(Schema.String, Schema.Unknown));

interface StorageCandidate {
  readonly baseDir: string;
  readonly preRootSegments: ReadonlyArray<string>;
  readonly postRootSegments: (sessionId: string) => ReadonlyArray<string>;
}

interface ResolvedCandidatePaths {
  readonly watchDir: string | undefined;
  readonly verifiedMessagesDir: string | undefined;
}

type VerifiedFileRead =
  | { readonly kind: "missing" }
  | { readonly kind: "empty" }
  | { readonly kind: "invalid" }
  | { readonly kind: "too_large" }
  | { readonly kind: "ok"; readonly contents: string; readonly mtimeMs: number };

interface DecodedMessageEntry {
  readonly fileName: string;
  readonly mtimeMs: number;
  readonly message: AntigravityMessage;
}

interface DirectoryScanResult {
  readonly readMap: Readonly<Record<string, boolean>>;
  readonly entries: ReadonlyArray<DecodedMessageEntry>;
}

interface TrackedToolCall {
  readonly toolName: string | undefined;
  readonly isCommand: boolean;
  readonly isSubagent: boolean;
  readonly isDaemon: boolean;
  readonly label: string | undefined;
}

interface TurnRoute {
  readonly threadId: ThreadId;
  readonly providerThreadId: ProviderThreadId;
}

export interface AntigravityBackgroundWake {
  readonly activateSession: (
    sessionId: string,
    options: { readonly seedExistingMessages: boolean },
  ) => Effect.Effect<void>;
  readonly observeSessionUpdate: (
    notification: EffectAcpSchema.SessionNotification,
  ) => Effect.Effect<void>;
  readonly onPromptStarted: () => void;
  readonly onPromptEnded: () => void;
  readonly onTurnStarting: (input: {
    readonly threadId: ThreadId;
    readonly providerThreadId: ProviderThreadId;
    readonly nativeSessionId: string | null;
  }) => Effect.Effect<void>;
  readonly onTurnStartFailed: () => Effect.Effect<void>;
  readonly onTurnTerminal: (
    status: "completed" | "interrupted" | "failed" | "cancelled",
  ) => Effect.Effect<void>;
  readonly onInterrupt: () => Effect.Effect<void>;
  readonly resetForReplacementSession: (nativeSessionId: string | null) => Effect.Effect<void>;
  readonly hasPendingBackgroundWork: Effect.Effect<boolean>;
}

export class CurrentAntigravityBackgroundWake extends Context.Reference<
  AntigravityBackgroundWake | undefined
>("CurrentAntigravityBackgroundWake", {
  defaultValue: () => undefined,
}) {}

export interface MakeAntigravityBackgroundWakeOptions {
  readonly profileDirectory?: string;
  readonly userHome?: string;
  readonly continuationRequests?: {
    readonly offer: (
      request: ProviderContinuationRequests.ProviderContinuationRequest,
    ) => Effect.Effect<void>;
  };
}

function isErrnoCode(cause: unknown, code: string): boolean {
  return typeof cause === "object" && cause !== null && "code" in cause && cause.code === code;
}

async function pathExistsOnDisk(targetPath: string): Promise<boolean> {
  try {
    await NodeFSP.lstat(targetPath);
    return true;
  } catch (cause) {
    return !isErrnoCode(cause, "ENOENT");
  }
}

async function resolveCandidatePaths(
  candidate: StorageCandidate,
  sessionId: string,
): Promise<ResolvedCandidatePaths> {
  if (!NATIVE_SESSION_ID.test(sessionId)) {
    return { watchDir: undefined, verifiedMessagesDir: undefined };
  }
  const baseRoot = await antigravityProfileRoot(candidate.baseDir);
  if (baseRoot === undefined) {
    return { watchDir: undefined, verifiedMessagesDir: undefined };
  }
  let verifiedRoot = baseRoot;
  if (candidate.preRootSegments.length > 0) {
    const preManaged = await antigravityManagedPath(
      baseRoot,
      candidate.preRootSegments,
      "directory",
    );
    if (preManaged === undefined) {
      const exists = await pathExistsOnDisk(NodePath.join(baseRoot, ...candidate.preRootSegments));
      return {
        watchDir: exists ? undefined : baseRoot,
        verifiedMessagesDir: undefined,
      };
    }
    verifiedRoot = preManaged.path;
  }
  const segments = candidate.postRootSegments(sessionId);
  let deepestDir = verifiedRoot;
  for (let index = 0; index < segments.length; index += 1) {
    const prefix = segments.slice(0, index + 1);
    const managed = await antigravityManagedPath(verifiedRoot, prefix, "directory");
    if (managed !== undefined) {
      deepestDir = managed.path;
      continue;
    }
    const exists = await pathExistsOnDisk(NodePath.join(deepestDir, segments[index]!));
    return {
      watchDir: exists ? undefined : deepestDir,
      verifiedMessagesDir: undefined,
    };
  }
  return { watchDir: deepestDir, verifiedMessagesDir: deepestDir };
}

async function readVerifiedTextFile(
  directoryPath: string,
  fileName: string,
): Promise<VerifiedFileRead> {
  const filePath = NodePath.join(directoryPath, fileName);
  let linkStat: NodeFS.Stats;
  try {
    linkStat = await NodeFSP.lstat(filePath);
  } catch (cause) {
    return isErrnoCode(cause, "ENOENT") ? { kind: "missing" } : { kind: "invalid" };
  }
  if (linkStat.isSymbolicLink() || !linkStat.isFile()) {
    return { kind: "invalid" };
  }
  if (linkStat.size === 0) {
    return { kind: "empty" };
  }
  if (linkStat.size > MAX_MESSAGE_FILE_BYTES) {
    return { kind: "too_large" };
  }
  const openFlags = NodeFS.constants.O_RDONLY | (NodeFS.constants.O_NOFOLLOW ?? 0);
  let handle: NodeFSP.FileHandle;
  try {
    handle = await NodeFSP.open(filePath, openFlags);
  } catch (cause) {
    return isErrnoCode(cause, "ENOENT") ? { kind: "missing" } : { kind: "invalid" };
  }
  try {
    const openedStat = await handle.stat();
    if (
      !openedStat.isFile() ||
      openedStat.dev !== linkStat.dev ||
      openedStat.ino !== linkStat.ino
    ) {
      return { kind: "invalid" };
    }
    if (openedStat.size === 0) {
      return { kind: "empty" };
    }
    if (openedStat.size > MAX_MESSAGE_FILE_BYTES) {
      return { kind: "too_large" };
    }
    const contents = await handle.readFile({ encoding: "utf8" });
    return contents.length === 0
      ? { kind: "empty" }
      : { kind: "ok", contents, mtimeMs: openedStat.mtimeMs };
  } catch {
    return { kind: "invalid" };
  } finally {
    await handle.close().catch(() => undefined);
  }
}

async function readDirectoryMessages(
  messagesDir: string,
): Promise<DirectoryScanResult | undefined> {
  const readJson = await readVerifiedTextFile(messagesDir, "read.json");
  let readMap: Readonly<Record<string, boolean>> = {};
  switch (readJson.kind) {
    case "missing":
      break;
    case "ok": {
      const decoded = decodeReadMap(readJson.contents);
      if (Option.isNone(decoded)) return undefined;
      readMap = decoded.value;
      break;
    }
    case "empty":
    case "invalid":
    case "too_large":
      return undefined;
  }

  const names = await NodeFSP.readdir(messagesDir).catch(() => undefined);
  if (names === undefined) return undefined;

  const entries: Array<DecodedMessageEntry> = [];
  for (const name of names) {
    if (name === "read.json" || !name.endsWith(".json")) continue;
    const fileRead = await readVerifiedTextFile(messagesDir, name);
    if (fileRead.kind !== "ok") continue;
    const decoded = decodeAntigravityMessage(fileRead.contents);
    if (Option.isNone(decoded)) continue;
    entries.push({
      fileName: name,
      mtimeMs: fileRead.mtimeMs,
      message: decoded.value,
    });
  }
  entries.sort((left, right) =>
    left.mtimeMs !== right.mtimeMs
      ? left.mtimeMs - right.mtimeMs
      : left.fileName.localeCompare(right.fileName),
  );
  return { readMap, entries };
}

function nonEmptyString(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function extractToolTextOutputs(
  update: EffectAcpSchema.SessionNotification["update"],
): Array<string> {
  const texts: Array<string> = [];
  if ("rawOutput" in update && typeof update.rawOutput === "string") {
    texts.push(update.rawOutput);
  }
  if ("content" in update && Array.isArray(update.content)) {
    for (const part of update.content) {
      if (
        typeof part === "object" &&
        part !== null &&
        "type" in part &&
        part.type === "content" &&
        "content" in part &&
        typeof part.content === "object" &&
        part.content !== null &&
        "type" in part.content &&
        part.content.type === "text" &&
        "text" in part.content &&
        typeof part.content.text === "string"
      ) {
        texts.push(part.content.text);
      }
    }
  }
  return texts;
}

function stripOutcomeSuffix(title: string | undefined): string | undefined {
  if (title === undefined) return undefined;
  const stripped = title.replace(TRAILING_OUTCOME_WORDS, "").trim();
  return stripped.length > 0 ? stripped : title;
}

export const makeAntigravityBackgroundWake = Effect.fn("AntigravityBackgroundWake.make")(function* (
  options: MakeAntigravityBackgroundWakeOptions,
): Effect.fn.Return<AntigravityBackgroundWake, never, Scope.Scope> {
  const candidates: ReadonlyArray<StorageCandidate> = [
    ...(options.profileDirectory === undefined
      ? []
      : [
          {
            baseDir: options.profileDirectory,
            preRootSegments: [],
            postRootSegments: (sessionId: string) => [
              "antigravity-acp",
              "brain",
              sessionId,
              ".system_generated",
              "messages",
            ],
          } satisfies StorageCandidate,
        ]),
    ...(options.userHome === undefined
      ? []
      : [
          {
            baseDir: options.userHome,
            preRootSegments: [".gemini"],
            postRootSegments: (sessionId: string) => [
              "antigravity-cli",
              "brain",
              sessionId,
              ".system_generated",
              "messages",
            ],
          } satisfies StorageCandidate,
        ]),
  ];

  const statePermit = yield* Semaphore.make(1);
  const wakeSignals = yield* Queue.sliding<void>(1);
  const watchers = new Map<number, { watchedPath: string; watcher: NodeFS.FSWatcher }>();

  let closed = false;
  let currentSessionId: string | null = null;
  let route: TurnRoute | null = null;
  let turnActive = false;
  let promptActive = false;
  let quarantined = false;
  let continuationRequested = false;
  let generation = 0;

  const seededSessions = new Set<string>();
  const seenMessageIds = new Set<string>();
  const toolInputsByCallId = new Map<string, TrackedToolCall>();
  const openCommandToolCallIds = new Set<string>();
  const completedToolCallIds = new Set<string>();
  const daemonToolCallIds = new Set<string>();
  const pendingTaskIds = new Set<string>();
  const completedTaskIds = new Set<string>();
  const killedTaskIds = new Set<string>();

  const closeAllWatchers = () => {
    for (const entry of watchers.values()) {
      try {
        entry.watcher.close();
      } catch {}
    }
    watchers.clear();
  };

  const signalWake = () => {
    if (!closed) {
      Queue.offerUnsafe(wakeSignals, undefined);
    }
  };

  const syncWatchersForResolved = (resolved: ReadonlyArray<ResolvedCandidatePaths>) => {
    if (closed) {
      closeAllWatchers();
      return;
    }
    for (let index = 0; index < candidates.length; index += 1) {
      const desiredPath = resolved[index]?.watchDir;
      const existing = watchers.get(index);
      if (existing?.watchedPath === desiredPath) continue;
      if (existing !== undefined) {
        try {
          existing.watcher.close();
        } catch {}
        watchers.delete(index);
      }
      if (desiredPath === undefined) continue;
      try {
        const watcher = NodeFS.watch(desiredPath, { persistent: false }, () => {
          signalWake();
        });
        watcher.on("error", () => {
          signalWake();
        });
        watchers.set(index, { watchedPath: desiredPath, watcher });
      } catch {}
    }
  };

  const isDaemonMessage = (message: AntigravityMessage): boolean => {
    const toolCall = message.sourceMetadata?.tool?.toolCall;
    if (toolCall?.id !== undefined && daemonToolCallIds.has(toolCall.id)) {
      return true;
    }
    const toolName =
      toolCall?.name ??
      (toolCall?.id === undefined ? undefined : toolInputsByCallId.get(toolCall.id)?.toolName);
    if (toolName !== "run_command" || toolCall?.argumentsJson === undefined) {
      return false;
    }
    const args = decodeArgumentsJson(toolCall.argumentsJson);
    return Option.isSome(args) && args.value.IsDaemon === true;
  };

  const toBackgroundWorkReport = (message: AntigravityMessage): BackgroundWorkReport => {
    const toolCall = message.sourceMetadata?.tool?.toolCall;
    const tracked = toolCall?.id === undefined ? undefined : toolInputsByCallId.get(toolCall.id);
    const decodedArgs =
      toolCall?.argumentsJson === undefined
        ? Option.none()
        : decodeArgumentsJson(toolCall.argumentsJson);
    const args = Option.getOrUndefined(decodedArgs);
    const toolName = toolCall?.name ?? tracked?.toolName;
    const content = message.content ?? "";
    const rawTitle = nonEmptyString(message.renderDetails?.messageTitle);
    const exitMatch = COMMAND_EXIT_CODE.exec(content);
    const exitCode = exitMatch === null ? undefined : Number(exitMatch[1]);
    const isCommand =
      toolName === "run_command" || tracked?.isCommand === true || exitCode !== undefined;
    const isSubagent =
      toolName === "invoke_subagent" ||
      toolName === "start_subagent" ||
      tracked?.isSubagent === true;

    const outcome: BackgroundWorkReport["outcome"] =
      exitCode !== undefined
        ? exitCode === 0
          ? "completed"
          : "failed"
        : /\bcancelled\b|\bkilled\b|\bstopped\b/i.test(rawTitle ?? "")
          ? "cancelled"
          : /\bfailed\b|\berror\b/i.test(rawTitle ?? "")
            ? "failed"
            : "completed";

    if (isCommand) {
      const label =
        nonEmptyString(args?.toolSummary) ??
        tracked?.label ??
        stripOutcomeSuffix(rawTitle) ??
        nonEmptyString(args?.CommandLine);
      return {
        kind: "command",
        outcome,
        ...(label === undefined ? {} : { label }),
        ...(exitCode === undefined ? {} : { exitCode }),
      };
    }

    if (isSubagent) {
      const label = nonEmptyString(args?.Role) ?? tracked?.label ?? stripOutcomeSuffix(rawTitle);
      return {
        kind: "subagent",
        outcome,
        ...(label === undefined ? {} : { label }),
      };
    }

    const label = stripOutcomeSuffix(rawTitle) ?? nonEmptyString(args?.Prompt) ?? tracked?.label;
    return {
      kind: "background_task",
      outcome,
      ...(label === undefined ? {} : { label }),
    };
  };

  const resolveAndReadSession = (sessionId: string) =>
    Effect.promise(async () => {
      const resolved = await Promise.all(
        candidates.map((candidate) => resolveCandidatePaths(candidate, sessionId)),
      );
      const scans = await Promise.all(
        resolved.map(async (item) =>
          item.verifiedMessagesDir === undefined
            ? undefined
            : readDirectoryMessages(item.verifiedMessagesDir),
        ),
      );
      return { resolved, scans };
    });

  // Directory watcher events can arrive in bursts per file write, so skip per-signal spans.
  const refreshWatchersAndScan = Effect.fnUntraced(function* () {
    const pendingOffer = yield* statePermit.withPermit(
      Effect.gen(function* () {
        if (closed || currentSessionId === null) {
          closeAllWatchers();
          return Option.none<{
            readonly route: TurnRoute;
            readonly offerGeneration: number;
            readonly reports: ReadonlyArray<BackgroundWorkReport>;
          }>();
        }
        const sessionId = currentSessionId;
        const { resolved, scans } = yield* resolveAndReadSession(sessionId);
        if (closed || currentSessionId !== sessionId) {
          return Option.none();
        }
        syncWatchersForResolved(resolved);
        if (turnActive) {
          return Option.none();
        }

        const wakeable: Array<{ readonly id: string; readonly report: BackgroundWorkReport }> = [];
        for (const scan of scans) {
          if (scan === undefined) continue;
          for (const { message } of scan.entries) {
            if (message.recipient !== undefined && message.recipient !== sessionId) {
              continue;
            }
            if (message.sender !== undefined) {
              pendingTaskIds.delete(message.sender);
              completedTaskIds.add(message.sender);
            }
            const toolCallId = message.sourceMetadata?.tool?.toolCall?.id;
            if (toolCallId !== undefined) {
              openCommandToolCallIds.delete(toolCallId);
              completedToolCallIds.add(toolCallId);
            }
            if (seenMessageIds.has(message.id)) continue;
            if (scan.readMap[message.id] === true) {
              seenMessageIds.add(message.id);
              continue;
            }
            if (quarantined) {
              seenMessageIds.add(message.id);
              continue;
            }
            if (
              message.priority !== "MESSAGE_PRIORITY_HIGH" ||
              message.hideFromUser === true ||
              isDaemonMessage(message) ||
              (message.sender !== undefined && killedTaskIds.has(message.sender))
            ) {
              seenMessageIds.add(message.id);
              continue;
            }
            wakeable.push({
              id: message.id,
              report: toBackgroundWorkReport(message),
            });
          }
        }

        if (
          wakeable.length === 0 ||
          continuationRequested ||
          route === null ||
          options.continuationRequests === undefined
        ) {
          return Option.none();
        }

        for (const item of wakeable) {
          seenMessageIds.add(item.id);
        }
        continuationRequested = true;
        generation += 1;
        return Option.some({
          route,
          offerGeneration: generation,
          reports: wakeable.map((item) => item.report),
        });
      }),
    );

    if (Option.isNone(pendingOffer) || options.continuationRequests === undefined) {
      return;
    }

    const { route: offerRoute, offerGeneration, reports } = pendingOffer.value;
    const notification = backgroundWorkNotification(reports);
    yield* options.continuationRequests.offer({
      threadId: offerRoute.threadId,
      providerThreadId: offerRoute.providerThreadId,
      driver: ANTIGRAVITY_PROVIDER,
      delivery: "message_text",
      detail: notification?.summary ?? null,
      ...(notification === null ? {} : { notification }),
      clearIfCurrent: () =>
        statePermit
          .withPermit(
            Effect.sync(() => {
              if (generation === offerGeneration) {
                continuationRequested = false;
              }
            }),
          )
          .pipe(Effect.tap(() => Effect.sync(signalWake))),
      dispatchIfCurrent: Effect.fn("AntigravityBackgroundWake.dispatchIfCurrent")(function* <
        A,
        E,
        R,
      >(effect: Effect.Effect<A, E, R>) {
        const admitted = yield* statePermit.withPermit(
          Effect.sync(() => {
            if (closed || quarantined) {
              if (generation === offerGeneration) {
                continuationRequested = false;
              }
              return false;
            }
            return generation === offerGeneration && continuationRequested;
          }),
        );
        if (!admitted) return Option.none();
        const exit = yield* Effect.exit(effect);
        if (Exit.isFailure(exit)) {
          yield* statePermit.withPermit(
            Effect.sync(() => {
              if (generation === offerGeneration) {
                continuationRequested = false;
              }
            }),
          );
          return yield* Effect.failCause(exit.cause);
        }
        return Option.some(exit.value);
      }),
    });
  });

  yield* Effect.addFinalizer(() =>
    statePermit.withPermit(
      Effect.sync(() => {
        closed = true;
        turnActive = false;
        promptActive = false;
        continuationRequested = false;
        generation += 1;
        closeAllWatchers();
      }),
    ),
  );

  yield* Stream.fromEffectRepeat(Queue.take(wakeSignals)).pipe(
    Stream.runForEach(() => refreshWatchersAndScan()),
    Effect.forkScoped,
  );

  const activateSession = Effect.fn("AntigravityBackgroundWake.activateSession")(function* (
    sessionId: string,
    activateOptions: { readonly seedExistingMessages: boolean },
  ) {
    yield* statePermit.withPermit(
      Effect.gen(function* () {
        if (closed || !NATIVE_SESSION_ID.test(sessionId)) return;
        currentSessionId = sessionId;
        const { resolved, scans } = yield* resolveAndReadSession(sessionId);
        if (closed || currentSessionId !== sessionId) return;
        syncWatchersForResolved(resolved);
        if (
          activateOptions.seedExistingMessages &&
          route === null &&
          !seededSessions.has(sessionId)
        ) {
          seededSessions.add(sessionId);
          for (const scan of scans) {
            if (scan === undefined) continue;
            for (const { message } of scan.entries) {
              seenMessageIds.add(message.id);
            }
          }
        }
      }),
    );
    signalWake();
  });

  // Session updates arrive on every streamed chunk, so skip per-chunk spans.
  const observeSessionUpdate = Effect.fnUntraced(function* (
    notification: EffectAcpSchema.SessionNotification,
  ) {
    yield* statePermit.withPermit(
      Effect.sync(() => {
        if (closed) return;
        const update = notification.update;
        if (update.sessionUpdate !== "tool_call" && update.sessionUpdate !== "tool_call_update") {
          return;
        }
        const toolCallId = update.toolCallId;
        const previous = toolInputsByCallId.get(toolCallId);
        const rawInput =
          "rawInput" in update ? Option.getOrUndefined(decodeRecord(update.rawInput)) : undefined;
        const rawTitle = "title" in update ? nonEmptyString(update.title) : undefined;
        const kind = "kind" in update && typeof update.kind === "string" ? update.kind : undefined;

        const toolName =
          rawTitle === "run_command" ||
          rawTitle === "manage_task" ||
          rawTitle === "invoke_subagent" ||
          rawTitle === "start_subagent" ||
          rawTitle === "schedule"
            ? rawTitle
            : previous?.toolName;

        const isCommand =
          previous?.isCommand === true ||
          kind === "execute" ||
          toolName === "run_command" ||
          typeof rawInput?.CommandLine === "string";

        const isSubagent =
          previous?.isSubagent === true ||
          toolName === "invoke_subagent" ||
          toolName === "start_subagent" ||
          (typeof rawInput?.TypeName === "string" && typeof rawInput?.Prompt === "string");

        const isDaemon = previous?.isDaemon === true || (isCommand && rawInput?.IsDaemon === true);

        const label =
          nonEmptyString(rawInput?.toolSummary) ??
          nonEmptyString(rawInput?.Role) ??
          previous?.label ??
          nonEmptyString(rawInput?.CommandLine);

        toolInputsByCallId.set(toolCallId, {
          toolName,
          isCommand,
          isSubagent,
          isDaemon,
          label,
        });

        if (
          rawInput?.Action === "kill" &&
          typeof rawInput.TaskId === "string" &&
          rawInput.TaskId.length > 0
        ) {
          killedTaskIds.add(rawInput.TaskId);
          pendingTaskIds.delete(rawInput.TaskId);
        }

        if (isDaemon) {
          daemonToolCallIds.add(toolCallId);
          openCommandToolCallIds.delete(toolCallId);
          return;
        }

        for (const outputText of extractToolTextOutputs(update)) {
          const match = BACKGROUND_TASK_OUTPUT.exec(outputText);
          const taskId = match?.[1];
          if (
            taskId !== undefined &&
            promptActive &&
            !killedTaskIds.has(taskId) &&
            !completedTaskIds.has(taskId)
          ) {
            pendingTaskIds.add(taskId);
          }
        }

        if (!isCommand) return;

        const status = "status" in update ? update.status : undefined;
        if (status === "completed" || status === "failed") {
          completedToolCallIds.add(toolCallId);
          const wasOpen = openCommandToolCallIds.delete(toolCallId);
          if (wasOpen && !turnActive && !quarantined) {
            signalWake();
          }
          return;
        }

        if (
          promptActive &&
          !completedToolCallIds.has(toolCallId) &&
          (update.sessionUpdate === "tool_call" || status === "pending" || status === "in_progress")
        ) {
          openCommandToolCallIds.add(toolCallId);
        }
      }),
    );
  });

  const onTurnStarting = Effect.fn("AntigravityBackgroundWake.onTurnStarting")(function* (input: {
    readonly threadId: ThreadId;
    readonly providerThreadId: ProviderThreadId;
    readonly nativeSessionId: string | null;
  }) {
    yield* statePermit.withPermit(
      Effect.gen(function* () {
        if (closed) return;
        route = {
          threadId: input.threadId,
          providerThreadId: input.providerThreadId,
        };
        turnActive = true;
        quarantined = false;
        continuationRequested = false;
        generation += 1;
        if (input.nativeSessionId !== null && NATIVE_SESSION_ID.test(input.nativeSessionId)) {
          currentSessionId = input.nativeSessionId;
          const { resolved } = yield* resolveAndReadSession(input.nativeSessionId);
          if (!closed && currentSessionId === input.nativeSessionId) {
            syncWatchersForResolved(resolved);
          }
        }
      }),
    );
  });

  const onTurnStartFailed = Effect.fn("AntigravityBackgroundWake.onTurnStartFailed")(function* () {
    yield* statePermit.withPermit(
      Effect.sync(() => {
        turnActive = false;
        promptActive = false;
      }),
    );
  });

  const onTurnTerminal = Effect.fn("AntigravityBackgroundWake.onTurnTerminal")(function* (
    status: "completed" | "interrupted" | "failed" | "cancelled",
  ) {
    if (status === "completed") {
      yield* statePermit.withPermit(
        Effect.sync(() => {
          turnActive = false;
          promptActive = false;
        }),
      );
      yield* refreshWatchersAndScan();
      return;
    }

    yield* statePermit.withPermit(
      Effect.sync(() => {
        turnActive = false;
        promptActive = false;
        quarantined = true;
        continuationRequested = false;
        generation += 1;
        if (status === "interrupted" || status === "cancelled") {
          openCommandToolCallIds.clear();
          pendingTaskIds.clear();
        }
      }),
    );
  });

  const onInterrupt = Effect.fn("AntigravityBackgroundWake.onInterrupt")(function* () {
    yield* statePermit.withPermit(
      Effect.sync(() => {
        turnActive = false;
        promptActive = false;
        quarantined = true;
        continuationRequested = false;
        generation += 1;
        openCommandToolCallIds.clear();
        pendingTaskIds.clear();
      }),
    );
  });

  const resetForReplacementSession = Effect.fn(
    "AntigravityBackgroundWake.resetForReplacementSession",
  )(function* (nativeSessionId: string | null) {
    yield* statePermit.withPermit(
      Effect.gen(function* () {
        if (closed) return;
        route = null;
        turnActive = false;
        promptActive = false;
        quarantined = false;
        continuationRequested = false;
        generation += 1;
        openCommandToolCallIds.clear();
        pendingTaskIds.clear();
        if (nativeSessionId === null || !NATIVE_SESSION_ID.test(nativeSessionId)) {
          currentSessionId = null;
          closeAllWatchers();
          return;
        }
        currentSessionId = nativeSessionId;
        const { resolved } = yield* resolveAndReadSession(nativeSessionId);
        if (!closed && currentSessionId === nativeSessionId) {
          syncWatchersForResolved(resolved);
        }
      }),
    );
  });

  const hasPendingBackgroundWork = statePermit.withPermit(
    Effect.sync(
      () =>
        !closed &&
        !quarantined &&
        (openCommandToolCallIds.size > 0 || pendingTaskIds.size > 0 || continuationRequested),
    ),
  );

  return {
    activateSession,
    observeSessionUpdate,
    onPromptStarted: () => {
      promptActive = true;
    },
    onPromptEnded: () => {
      promptActive = false;
    },
    onTurnStarting,
    onTurnStartFailed,
    onTurnTerminal,
    onInterrupt,
    resetForReplacementSession,
    hasPendingBackgroundWork,
  };
});
