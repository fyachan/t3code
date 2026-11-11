import {
  ANTIGRAVITY_DEFAULT_MODEL,
  ProviderDriverKind,
  ProviderInstanceId,
  type OrchestrationV2ProviderCapabilities,
  type ProviderSetupError,
} from "@t3tools/contracts";
import type { SelfInvocation } from "@t3tools/shared/nodeRuntime";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Exit from "effect/Exit";
import * as Semaphore from "effect/Semaphore";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as EffectAcpErrors from "effect-acp/errors";

import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderHost from "@t3tools/provider-core/server/ProviderHost";
import { PULL_REQUEST_LINKING_INSTRUCTIONS } from "@t3tools/provider-core/server/runtimeInstructions";
import type { AntigravityAuth } from "../../provider/AntigravityAuth.ts";
import type * as AcpSessionRuntime from "@t3tools/provider-acp/server/AcpSessionRuntime";
import {
  CurrentAntigravityBackgroundWake,
  makeAntigravityBackgroundWake,
} from "../../provider/acp/AntigravityBackgroundWake.ts";
import {
  antigravityAppendsSystemPrompt,
  antigravityPermissionMode,
  antigravityRuntimeModeForSessionMode,
  applyAntigravityAcpModelSelection,
  buildAntigravityPrompt,
  type AntigravityAcpRuntimeInput,
} from "../../provider/acp/AntigravityAcpSupport.ts";
import {
  ANTIGRAVITY_SUBAGENT_BATCH_TITLE,
  antigravitySubagentLaunches,
  describeAntigravitySubagentLaunches,
} from "../../provider/acp/AntigravityChildWork.ts";
import { readAntigravityClientTextFile } from "../../provider/acp/AntigravityClientFiles.ts";
import { watchAntigravityPromptOutcome } from "../../provider/acp/AntigravityTurnOutcome.ts";
import {
  antigravityApprovalOptions,
  antigravitySubagentOutput,
  classifyAntigravitySubagentToolCall,
  extractAntigravityUserInputQuestion,
  makeAntigravityReadRepair,
  makeAntigravityUserInputResponse,
  normalizeAntigravityToolCall,
} from "../../provider/acp/AntigravityProtocol.ts";
import { isAntigravitySignInRequiredError } from "../../provider/antigravityAuthSupport.ts";
import {
  AcpProviderCapabilitiesV2,
  makeAcpAdapterV2,
  type AcpAdapterV2Flavor,
  type AcpAdapterV2RuntimeInput,
} from "@t3tools/provider-acp/server/adapter";

const ANTIGRAVITY_PROVIDER = ProviderDriverKind.make("antigravity");

const AntigravityProviderCapabilitiesV2 = {
  ...AcpProviderCapabilitiesV2,
  sessions: {
    ...AcpProviderCapabilitiesV2.sessions,
    supportsModelSwitchInSession: true,
    supportsRuntimeModeSwitchInSession: true,
  },
  tools: {
    ...AcpProviderCapabilitiesV2.tools,
    supportsMcpTools: true,
  },
  subagents: {
    ...AcpProviderCapabilitiesV2.subagents,
    supportsSubagents: true,
  },
} satisfies OrchestrationV2ProviderCapabilities;

export interface AntigravityAdapterV2Options {
  readonly instanceId: ProviderInstanceId;
  readonly selfInvocation: SelfInvocation;
  readonly makeRuntime: (
    input: Omit<AntigravityAcpRuntimeInput, "spawn" | "childProcessSpawner">,
  ) => Effect.Effect<
    AcpSessionRuntime.AcpSessionRuntime["Service"],
    EffectAcpErrors.AcpError | ProviderSetupError,
    Scope.Scope
  >;
  readonly withProcess: AntigravityAuth["withProcess"];
  readonly defaultModel: Effect.Effect<string | undefined>;
  readonly onSessionStarted?: (
    started: AcpSessionRuntime.AcpSessionRuntimeStartResult,
    cwd: string,
  ) => Effect.Effect<void>;
  readonly onSessionEvent?: AcpAdapterV2Flavor["onSessionEvent"];
  readonly nativeLogging?: Parameters<typeof makeAcpAdapterV2>[0]["nativeLogging"];
  readonly continuationRequests?: Parameters<typeof makeAcpAdapterV2>[0]["continuationRequests"];
  /** The agent's GEMINI_HOME. Its session logs show turns that ended on an upstream error. */
  readonly profileDirectory?: string;
  /** The OS user home whose `.gemini/antigravity-cli` holds native CLI session state. */
  readonly userHome?: string;
}

/**
 * Native subagent tools report no child session ids, so children stay read-only.
 * Only permission prompts carry the launch; NativeChildWorkQuery matches their ids by this text.
 */
const extractAntigravitySubagentUpdate: NonNullable<AcpAdapterV2Flavor["extractSubagentUpdate"]> = (
  toolCall,
) => {
  if (classifyAntigravitySubagentToolCall(toolCall) !== "subagent") {
    return undefined;
  }
  const status =
    toolCall.status === "failed" ? "failed" : toolCall.status === "pending" ? "pending" : "running";
  const launches = antigravitySubagentLaunches(toolCall.data.rawInput);
  const launch = launches === undefined ? undefined : describeAntigravitySubagentLaunches(launches);
  return {
    nativeTaskId: toolCall.toolCallId,
    prompt:
      launch?.prompt ??
      antigravitySubagentOutput(toolCall) ??
      toolCall.detail ??
      toolCall.title ??
      ANTIGRAVITY_SUBAGENT_BATCH_TITLE,
    title: launch?.title ?? ANTIGRAVITY_SUBAGENT_BATCH_TITLE,
    model: null,
    status,
    childSessionId: null,
    result: status === "failed" ? (antigravitySubagentOutput(toolCall) ?? null) : null,
  };
};

interface AntigravityFlavorServices {
  readonly random: SecureRandom.SecureRandom["Service"];
  readonly fileSystem: FileSystem.FileSystem;
  readonly path: Path.Path;
  readonly host: ProviderHost.ProviderHost["Service"];
}

export function makeAntigravityAcpAdapterFlavor(
  options: AntigravityAdapterV2Options & AntigravityFlavorServices,
): AcpAdapterV2Flavor {
  const { profileDirectory } = options;
  const antigravityClientFileRoots = (cwd: string | null) =>
    cwd === null ? [options.host.paths.attachmentsDir] : [cwd, options.host.paths.attachmentsDir];
  const makeRuntime = Effect.fn("AntigravityAdapterV2.makeRuntime")(
    function* (input: AcpAdapterV2RuntimeInput) {
      const backgroundWake = yield* CurrentAntigravityBackgroundWake;
      const scope = yield* Scope.fork(yield* Effect.scope);
      const runtime = yield* options
        .withProcess(
          Scope.close(scope, Exit.void),
          options.makeRuntime({
            ...input,
            clientFileReads: true,
            sessionSystemPromptAppend: PULL_REQUEST_LINKING_INSTRUCTIONS,
            additionalDirectories: [options.host.paths.attachmentsDir],
          }),
        )
        .pipe(Effect.provideService(Scope.Scope, scope));
      // Repair keeps state across updates, so each runtime must have one session-update handler.
      const reads = makeAntigravityReadRepair();
      // Served reads are reported from the read handler, so they take turns with agent updates.
      const updates = yield* Semaphore.make(1);
      let projectUpdate: Parameters<typeof runtime.handleSessionUpdate>[0] | undefined;
      return {
        ...runtime,
        handleReadTextFile: (handler) =>
          runtime.handleReadTextFile((request, context) =>
            handler(request, context).pipe(
              Effect.onExit((exit) => {
                const served = reads.attempted(request.path, Exit.isSuccess(exit));
                if (served === undefined || projectUpdate === undefined) return Effect.void;
                return updates
                  .withPermit(
                    projectUpdate({
                      sessionId: served.sessionId,
                      update: {
                        sessionUpdate: "tool_call_update",
                        toolCallId: served.toolCallId,
                        status: "completed",
                      },
                    }),
                  )
                  .pipe(Effect.ignore);
              }),
            ),
          ),
        prompt: (payload, promptOptions) =>
          Effect.sync(() => {
            reads.reset();
            backgroundWake?.onPromptStarted();
          }).pipe(
            Effect.andThen(runtime.prompt(payload, promptOptions)),
            Effect.ensuring(
              Effect.sync(() => {
                backgroundWake?.onPromptEnded();
              }),
            ),
          ),
        handleSessionUpdate: (handler) => {
          projectUpdate = handler;
          return runtime.handleSessionUpdate((notification) =>
            updates.withPermit(
              Effect.gen(function* () {
                const repaired = reads.repair(notification);
                if (backgroundWake !== undefined) {
                  yield* backgroundWake.observeSessionUpdate(repaired);
                }
                yield* handler(repaired);
              }),
            ),
          );
        },
        start: () =>
          runtime.start().pipe(
            Effect.tap((started) =>
              (backgroundWake === undefined
                ? Effect.void
                : backgroundWake.activateSession(started.sessionId, {
                    seedExistingMessages: input.resumeSessionId !== undefined,
                  })
              ).pipe(Effect.andThen(options.onSessionStarted?.(started, input.cwd) ?? Effect.void)),
            ),
          ),
        loadSession: (sessionId, activationOptions) =>
          runtime.loadSession(sessionId, activationOptions).pipe(
            Effect.tap((started) =>
              backgroundWake === undefined
                ? Effect.void
                : backgroundWake.activateSession(started.sessionId, {
                    seedExistingMessages: true,
                  }),
            ),
          ),
        resumeSession: (sessionId, activationOptions) =>
          runtime.resumeSession(sessionId, activationOptions).pipe(
            Effect.tap((started) =>
              backgroundWake === undefined
                ? Effect.void
                : backgroundWake.activateSession(started.sessionId, {
                    seedExistingMessages: true,
                  }),
            ),
          ),
        forkSession: (sessionId, activationOptions, meta) =>
          runtime.forkSession(sessionId, activationOptions, meta).pipe(
            Effect.tap((started) =>
              backgroundWake === undefined
                ? Effect.void
                : backgroundWake.activateSession(started.sessionId, {
                    seedExistingMessages: true,
                  }),
            ),
          ),
      } satisfies typeof runtime;
    },
    Effect.provideService(SecureRandom.SecureRandom, options.random),
    Effect.mapError((cause): EffectAcpErrors.AcpError =>
      cause._tag === "ProviderSetupError"
        ? new EffectAcpErrors.AcpTransportError({ detail: cause.detail, cause })
        : cause,
    ),
  );

  return {
    driver: ANTIGRAVITY_PROVIDER,
    runtimeHarness: "Antigravity",
    capabilities: AntigravityProviderCapabilitiesV2,
    // Agents that advertise HTTP MCP skip the stdio bridge process; others keep stdio.
    mcpOverHttp: true,
    sessionCarriesPullRequestLinking: antigravityAppendsSystemPrompt,
    makeRuntime,
    buildPrompt: ({
      text,
      attachments,
      resolveAttachmentPath,
      promptCapabilities,
      agentVersion,
      currentModel,
    }) =>
      buildAntigravityPrompt({
        input: text,
        attachments,
        resolveAttachmentPath,
        promptCapabilities,
        agentVersion,
        currentModel,
      }).pipe(
        Effect.provideService(FileSystem.FileSystem, options.fileSystem),
        Effect.provideService(Path.Path, options.path),
      ),
    // Loading history replays tool calls; resume avoids replay.
    subagentsIdleOnTurnCompletion: true,
    ...(options.onSessionEvent === undefined ? {} : { onSessionEvent: options.onSessionEvent }),
    applyModelSelection: Effect.fn("AntigravityAdapterV2.applyModelSelection")(function* ({
      runtime,
      modelSelection,
    }) {
      const defaultModel = yield* options.defaultModel;
      return yield* applyAntigravityAcpModelSelection({
        runtime,
        model:
          modelSelection.model === ANTIGRAVITY_DEFAULT_MODEL ? undefined : modelSelection.model,
        defaultModel,
        mapError: (cause) => cause,
      });
    }),
    sessionModeForPolicy: (policy) => antigravityPermissionMode(policy.runtimeMode),
    runtimeModeForSessionMode: antigravityRuntimeModeForSessionMode,
    clientFileSystem: {
      readTextFile: (request, cwd) =>
        readAntigravityClientTextFile({
          fileSystem: options.fileSystem,
          path: options.path,
          allowedRoots: antigravityClientFileRoots(cwd),
          request,
        }),
    },
    approvalOptions: antigravityApprovalOptions,
    extractPermissionQuestion: (request) => {
      const question = extractAntigravityUserInputQuestion(request);
      if (question === undefined) return undefined;
      return {
        question,
        respond: (answers) => makeAntigravityUserInputResponse(request, answers),
      };
    },
    normalizeToolCall: normalizeAntigravityToolCall,
    extractSubagentUpdate: extractAntigravitySubagentUpdate,
    supportsImagePrompts: true,
    // ACP 1.3.0 treats /compact as ordinary model input.
    supportsCompaction: false,
    requiresSignIn: isAntigravitySignInRequiredError,
    // Antigravity can leave a prompt open without updates; long tools and approvals pause this.
    promptInactivityTimeout: "15 minutes",
    // 1.4.5 returns the root trajectory's Gemini usageMetadata delta; 1.4.1 returns none.
    reportsPerTurnPromptUsage: true,
    geminiPromptUsage: true,
    ...(profileDirectory === undefined
      ? {}
      : {
          watchPromptOutcome: (sessionId: string) =>
            watchAntigravityPromptOutcome(profileDirectory, sessionId),
        }),
  };
}

export const makeAntigravityAdapterV2 = Effect.fn("makeAntigravityAdapterV2")(function* (
  options: AntigravityAdapterV2Options,
) {
  const services: AntigravityFlavorServices = {
    random: yield* SecureRandom.SecureRandom,
    fileSystem: yield* FileSystem.FileSystem,
    path: yield* Path.Path,
    host: yield* ProviderHost.ProviderHost,
  };
  const baseAdapter = yield* makeAcpAdapterV2({
    instanceId: options.instanceId,
    flavor: makeAntigravityAcpAdapterFlavor({ ...options, ...services }),
    selfInvocation: options.selfInvocation,
    ...(options.nativeLogging === undefined ? {} : { nativeLogging: options.nativeLogging }),
    ...(options.continuationRequests === undefined
      ? {}
      : { continuationRequests: options.continuationRequests }),
  });
  return ProviderAdapter.ProviderAdapterV2.of({
    ...baseAdapter,
    openSession: Effect.fn("AntigravityAdapterV2.openSession")(function* (input) {
      const wake = yield* makeAntigravityBackgroundWake({
        ...(options.profileDirectory === undefined
          ? {}
          : { profileDirectory: options.profileDirectory }),
        ...(options.userHome === undefined ? {} : { userHome: options.userHome }),
        ...(options.continuationRequests === undefined
          ? {}
          : { continuationRequests: options.continuationRequests }),
      });
      const withWake = Effect.provideService(CurrentAntigravityBackgroundWake, wake);
      const baseSession = yield* baseAdapter.openSession(input).pipe(withWake);
      const baseHasPendingBackgroundWork = baseSession.hasPendingBackgroundWork;
      return {
        ...baseSession,
        events: baseSession.events.pipe(
          Stream.tap((event) =>
            event.type === "turn.terminal" ? wake.onTurnTerminal(event.status) : Effect.void,
          ),
        ),
        hasPendingBackgroundWork:
          baseHasPendingBackgroundWork === undefined
            ? wake.hasPendingBackgroundWork
            : Effect.gen(function* () {
                return (
                  (yield* baseHasPendingBackgroundWork) || (yield* wake.hasPendingBackgroundWork)
                );
              }),
        ensureThread: (threadInput) =>
          baseSession.ensureThread(threadInput).pipe(
            withWake,
            Effect.tap((providerThread) => {
              const nativeId = providerThread.nativeThreadRef?.nativeId;
              return nativeId == null
                ? Effect.void
                : wake.activateSession(nativeId, { seedExistingMessages: false });
            }),
          ),
        resumeThread: (threadInput) =>
          baseSession.resumeThread(threadInput).pipe(
            withWake,
            Effect.tap((providerThread) => {
              const nativeId = providerThread.nativeThreadRef?.nativeId;
              return nativeId == null
                ? Effect.void
                : wake.activateSession(nativeId, { seedExistingMessages: true });
            }),
          ),
        startTurn: (turnInput) =>
          wake
            .onTurnStarting({
              threadId: turnInput.threadId,
              providerThreadId: turnInput.providerThread.id,
              nativeSessionId: turnInput.providerThread.nativeThreadRef?.nativeId ?? null,
            })
            .pipe(
              Effect.andThen(
                baseSession.startTurn(turnInput).pipe(
                  withWake,
                  Effect.tapError(() => wake.onTurnStartFailed()),
                ),
              ),
            ),
        interruptTurn: (turnInput) =>
          wake
            .onInterrupt()
            .pipe(Effect.andThen(baseSession.interruptTurn(turnInput).pipe(withWake))),
        readThreadSnapshot: (snapshotInput) =>
          baseSession.readThreadSnapshot(snapshotInput).pipe(withWake),
        rollbackThread: (rollbackInput) =>
          baseSession.rollbackThread(rollbackInput).pipe(
            withWake,
            Effect.tap((rolledBack) =>
              wake.resetForReplacementSession(
                rolledBack.providerThread.nativeThreadRef?.nativeId ?? null,
              ),
            ),
          ),
        forkThread: (forkInput) =>
          baseSession.forkThread(forkInput).pipe(
            withWake,
            Effect.tap((forked) =>
              wake.resetForReplacementSession(forked.nativeThreadRef?.nativeId ?? null),
            ),
          ),
      };
    }),
  });
});
