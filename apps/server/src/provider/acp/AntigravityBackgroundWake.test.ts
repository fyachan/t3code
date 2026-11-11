import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderThreadId, ThreadId } from "@t3tools/contracts";
import type * as ProviderContinuationRequests from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Queue from "effect/Queue";

import { makeAntigravityBackgroundWake } from "./AntigravityBackgroundWake.ts";

const THREAD_ID = ThreadId.make("thread-bg-wake-unit");
const PROVIDER_THREAD_ID = ProviderThreadId.make("provider-thread-bg-wake-unit");

describe("makeAntigravityBackgroundWake", () => {
  it.effect(
    "watches the userHome .gemini/antigravity-cli layout even when .gemini is created after the turn settles",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const userHome = yield* fs.makeTempDirectoryScoped({ prefix: "t3-agy-home-" });
        const offered =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const sessionId = "cli-session-1";

        const wake = yield* makeAntigravityBackgroundWake({
          userHome,
          continuationRequests: {
            offer: (request) => Queue.offer(offered, request).pipe(Effect.asVoid),
          },
        });

        yield* wake.activateSession(sessionId, { seedExistingMessages: false });
        yield* wake.onTurnStarting({
          threadId: THREAD_ID,
          providerThreadId: PROVIDER_THREAD_ID,
          nativeSessionId: sessionId,
        });
        wake.onPromptStarted();
        yield* wake.observeSessionUpdate({
          sessionId,
          update: {
            sessionUpdate: "tool_call",
            toolCallId: "call_cli_bg",
            title: "run_command",
            kind: "execute",
            status: "in_progress",
            rawInput: {
              CommandLine: "git clone https://example.com/repo.git",
              WaitMsBeforeAsync: 5000,
              toolSummary: "Clone repository",
            },
          },
        });
        wake.onPromptEnded();
        yield* wake.onTurnTerminal("completed");

        assert.isTrue(yield* wake.hasPendingBackgroundWork);

        const cliMessagesDir = path.join(
          userHome,
          ".gemini",
          "antigravity-cli",
          "brain",
          sessionId,
          ".system_generated",
          "messages",
        );
        yield* fs.makeDirectory(cliMessagesDir, { recursive: true });
        yield* fs.writeFileString(
          path.join(cliMessagesDir, "msg-cli-1.json"),
          JSON.stringify({
            id: "msg-cli-1",
            recipient: sessionId,
            sender: `${sessionId}/task-1`,
            priority: "MESSAGE_PRIORITY_HIGH",
            renderDetails: { messageTitle: "Clone repository finished" },
            content: `Task id "${sessionId}/task-1" finished with result:\nCommand exited with code 0.\n`,
            sourceMetadata: {
              tool: {
                toolCall: {
                  id: "call_cli_bg",
                  name: "run_command",
                  argumentsJson: JSON.stringify({
                    CommandLine: "git clone https://example.com/repo.git",
                    WaitMsBeforeAsync: 5000,
                    toolSummary: "Clone repository",
                  }),
                },
              },
            },
          }),
        );

        const request = yield* Queue.take(offered);
        assert.equal(request.delivery, "message_text");
        assert.deepEqual(request.notification, {
          source: { kind: "command" },
          outcome: "completed",
          summary: 'Command "Clone repository" finished (exit 0)',
        });
      }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect(
    "ignores read.json consumed messages, daemon commands, low-priority or hidden messages, killed tasks, and symlinks",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const profileDirectory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-agy-filter-" });
        const externalDir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-agy-external-" });
        const offered =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const sessionId = "filter-session-1";

        const messagesDir = path.join(
          profileDirectory,
          "antigravity-acp",
          "brain",
          sessionId,
          ".system_generated",
          "messages",
        );
        yield* fs.makeDirectory(messagesDir, { recursive: true });

        const wake = yield* makeAntigravityBackgroundWake({
          profileDirectory,
          continuationRequests: {
            offer: (request) => Queue.offer(offered, request).pipe(Effect.asVoid),
          },
        });

        yield* wake.activateSession(sessionId, { seedExistingMessages: false });
        yield* wake.onTurnStarting({
          threadId: THREAD_ID,
          providerThreadId: PROVIDER_THREAD_ID,
          nativeSessionId: sessionId,
        });
        wake.onPromptStarted();

        // A daemon command must not pin hasPendingBackgroundWork or trigger a wake.
        yield* wake.observeSessionUpdate({
          sessionId,
          update: {
            sessionUpdate: "tool_call",
            toolCallId: "call_daemon_server",
            title: "run_command",
            kind: "execute",
            status: "in_progress",
            rawInput: {
              CommandLine: "pnpm dev",
              IsDaemon: true,
              WaitMsBeforeAsync: 1000,
              toolSummary: "Dev server startup",
            },
          },
        });

        // A killed task must not trigger a wake even if a message arrives for its sender.
        yield* wake.observeSessionUpdate({
          sessionId,
          update: {
            sessionUpdate: "tool_call",
            toolCallId: "call_kill_task",
            title: "manage_task",
            status: "completed",
            rawInput: {
              Action: "kill",
              TaskId: `${sessionId}/task-killed`,
            },
          },
        });

        // Message already consumed in-turn and marked in read.json.
        yield* fs.writeFileString(
          path.join(messagesDir, "01-consumed.json"),
          JSON.stringify({
            id: "msg-consumed",
            recipient: sessionId,
            sender: `${sessionId}/task-consumed`,
            priority: "MESSAGE_PRIORITY_HIGH",
            content: "Command exited with code 0.\n",
          }),
        );
        yield* fs.writeFileString(
          path.join(messagesDir, "read.json"),
          JSON.stringify({ "msg-consumed": true }),
        );

        // Low-priority and hidden messages.
        yield* fs.writeFileString(
          path.join(messagesDir, "02-low.json"),
          JSON.stringify({
            id: "msg-low",
            recipient: sessionId,
            sender: `${sessionId}/task-low`,
            priority: "MESSAGE_PRIORITY_LOW",
            hideFromUser: true,
            content: "Killed task",
          }),
        );

        // Daemon completion message.
        yield* fs.writeFileString(
          path.join(messagesDir, "03-daemon.json"),
          JSON.stringify({
            id: "msg-daemon",
            recipient: sessionId,
            sender: `${sessionId}/task-daemon`,
            priority: "MESSAGE_PRIORITY_HIGH",
            content: "Command exited with code 0.\n",
            sourceMetadata: {
              tool: {
                toolCall: {
                  id: "call_daemon_server",
                  name: "run_command",
                  argumentsJson: JSON.stringify({
                    CommandLine: "pnpm dev",
                    IsDaemon: true,
                    toolSummary: "Dev server startup",
                  }),
                },
              },
            },
          }),
        );

        // Explicitly killed task message.
        yield* fs.writeFileString(
          path.join(messagesDir, "04-killed.json"),
          JSON.stringify({
            id: "msg-killed",
            recipient: sessionId,
            sender: `${sessionId}/task-killed`,
            priority: "MESSAGE_PRIORITY_HIGH",
            content: "Command exited with code 143.\n",
          }),
        );

        // Symlinked message file pointing outside the managed directory.
        const externalTarget = path.join(externalDir, "forged.json");
        yield* fs.writeFileString(
          externalTarget,
          JSON.stringify({
            id: "msg-symlink-forged",
            recipient: sessionId,
            priority: "MESSAGE_PRIORITY_HIGH",
            content: "Command exited with code 0.\n",
          }),
        );
        yield* fs.symlink(externalTarget, path.join(messagesDir, "05-forged.json"));

        wake.onPromptEnded();
        yield* wake.onTurnTerminal("completed");

        assert.isFalse(yield* wake.hasPendingBackgroundWork);
        assert.equal(yield* Queue.size(offered), 0);

        // A genuine high-priority subagent completion afterwards still wakes cleanly.
        yield* fs.writeFileString(
          path.join(messagesDir, "06-real-subagent.json"),
          JSON.stringify({
            id: "msg-real-subagent",
            recipient: sessionId,
            sender: "subagent-conv-1",
            priority: "MESSAGE_PRIORITY_HIGH",
            renderDetails: { messageTitle: "Codebase Researcher finished" },
            content: "Subagent report ready.",
            sourceMetadata: {
              tool: {
                toolCall: {
                  id: "call_subagent_1",
                  name: "invoke_subagent",
                  argumentsJson: JSON.stringify({
                    TypeName: "research",
                    Role: "Codebase Researcher",
                    Prompt: "Inspect adapters",
                  }),
                },
              },
            },
          }),
        );

        const request = yield* Queue.take(offered);
        assert.deepEqual(request.notification, {
          source: { kind: "subagent" },
          outcome: "completed",
          summary: 'Subagent "Codebase Researcher" finished',
        });
      }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );

  it.effect(
    "quarantines pending wakes on interrupt and seeds pre-existing messages on session resume",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const profileDirectory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-agy-stop-" });
        const offered =
          yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
        const sessionId = "resumed-session-1";

        const messagesDir = path.join(
          profileDirectory,
          "antigravity-acp",
          "brain",
          sessionId,
          ".system_generated",
          "messages",
        );
        yield* fs.makeDirectory(messagesDir, { recursive: true });
        yield* fs.writeFileString(
          path.join(messagesDir, "msg-historical.json"),
          JSON.stringify({
            id: "msg-historical",
            recipient: sessionId,
            priority: "MESSAGE_PRIORITY_HIGH",
            content: "Command exited with code 0.\n",
          }),
        );

        const wake = yield* makeAntigravityBackgroundWake({
          profileDirectory,
          continuationRequests: {
            offer: (request) => Queue.offer(offered, request).pipe(Effect.asVoid),
          },
        });

        // Seeding on resume ignores historical unread messages already on disk.
        yield* wake.activateSession(sessionId, { seedExistingMessages: true });
        yield* wake.onTurnStarting({
          threadId: THREAD_ID,
          providerThreadId: PROVIDER_THREAD_ID,
          nativeSessionId: sessionId,
        });
        wake.onPromptStarted();
        wake.onPromptEnded();
        yield* wake.onTurnTerminal("completed");
        assert.equal(yield* Queue.size(offered), 0);

        // A new message after the turn settles offers a continuation, which Stop then quarantines.
        yield* fs.writeFileString(
          path.join(messagesDir, "msg-after-turn.json"),
          JSON.stringify({
            id: "msg-after-turn",
            recipient: sessionId,
            priority: "MESSAGE_PRIORITY_HIGH",
            renderDetails: { messageTitle: "Unit test execution failed" },
            content: "Command exited with code 1.\n",
            sourceMetadata: {
              tool: {
                toolCall: {
                  id: "call_test_fail",
                  name: "run_command",
                  argumentsJson: JSON.stringify({
                    CommandLine: "vp test",
                    toolSummary: "Unit test execution",
                  }),
                },
              },
            },
          }),
        );

        const request = yield* Queue.take(offered);
        assert.deepEqual(request.notification, {
          source: { kind: "command" },
          outcome: "failed",
          summary: 'Command "Unit test execution" failed (exit 1)',
        });
        assert.isTrue(yield* wake.hasPendingBackgroundWork);

        yield* wake.onInterrupt();
        assert.isFalse(yield* wake.hasPendingBackgroundWork);
        const dispatchedAfterInterrupt = yield* request.dispatchIfCurrent!(Effect.succeed("ran"));
        assert.isTrue(Option.isNone(dispatchedAfterInterrupt));
      }).pipe(Effect.provide(NodeServices.layer), Effect.scoped),
  );
});
