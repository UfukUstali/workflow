import { getConvexSize, v } from "convex/values";
import { mutation, query, type MutationCtx } from "./_generated/server.js";
import {
  journalDocument,
  type JournalEntry,
  step,
  workflowDocument,
} from "./schema.js";
import { getWorkflow } from "./model.js";
import { logLevel } from "./logging.js";
import { vRetryBehavior, type WorkId } from "@convex-dev/workpool";
import {
  getWorkpool,
  type OnCompleteContext,
  workpoolOptions,
} from "./pool.js";
import { internal } from "./_generated/api.js";
import { createFunctionHandle, type FunctionHandle } from "convex/server";
import { getDefaultLogger, runResultToError } from "./utils.js";
import { assert } from "convex-helpers";
import { MAX_JOURNAL_SIZE } from "../shared.js";
import { awaitEvent } from "./event.js";
import { createHandler } from "./workflow.js";
import type { Doc } from "./_generated/dataModel.js";

type SentEvent = Doc<"events"> & {
  state: { kind: "sent" };
};

async function sentEventsByNames(
  ctx: MutationCtx,
  workflowId: Doc<"workflows">["_id"],
  events: Array<{ name: string }>,
) {
  return (
    (await ctx.db
      .query("events")
      .withIndex("workflowId_state", (q) =>
        q.eq("workflowId", workflowId).eq("state.kind", "sent"),
      )
      .filter((q) => q.or(...events.map((e) => q.eq(q.field("name"), e.name))))
      .collect()) as SentEvent[]
  ).sort((a, b) => a.state.sentAt - b.state.sentAt);
}

export const load = query({
  args: {
    workflowId: v.id("workflows"),
    shortCircuit: v.optional(v.boolean()),
  },
  returns: v.object({
    workflow: workflowDocument,
    journalEntries: v.array(journalDocument),
    ok: v.boolean(),
    logLevel,
    blocked: v.optional(v.boolean()),
  }),
  handler: async (ctx, { workflowId, shortCircuit }) => {
    const workflow = await ctx.db.get(workflowId);
    assert(workflow, `Workflow not found: ${workflowId}`);
    const { logLevel } = await getDefaultLogger(ctx);
    const journalEntries: JournalEntry[] = [];
    let journalSize = 0;
    if (shortCircuit) {
      const inProgress = await ctx.db
        .query("steps")
        .withIndex("inProgress", (q) =>
          q.eq("step.inProgress", true).eq("workflowId", workflowId),
        )
        .first();
      if (inProgress) {
        return {
          journalEntries: [inProgress],
          blocked: true,
          workflow,
          logLevel,
          ok: true,
        };
      }
    }
    for await (const entry of ctx.db
      .query("steps")
      .withIndex("workflow", (q) => q.eq("workflowId", workflowId))) {
      journalEntries.push(entry);
      journalSize += getConvexSize(entry);
      if (journalSize > MAX_JOURNAL_SIZE) {
        return { journalEntries, workflow, logLevel, ok: false };
      }
    }
    return { journalEntries, workflow, logLevel, ok: true };
  },
});

export const startSteps = mutation({
  args: {
    workflowId: v.id("workflows"),
    generationNumber: v.number(),
    steps: v.array(
      v.object({
        step,
        retry: v.optional(v.union(v.boolean(), vRetryBehavior)),
        schedulerOptions: v.optional(
          v.union(
            v.object({ runAt: v.optional(v.number()) }),
            v.object({ runAfter: v.optional(v.number()) }),
          ),
        ),
      }),
    ),
    workpoolOptions: v.optional(workpoolOptions),
  },
  returns: v.array(journalDocument),
  handler: async (ctx, args): Promise<JournalEntry[]> => {
    const { generationNumber } = args;
    const workflow = await getWorkflow(ctx, args.workflowId, generationNumber);
    const console = await getDefaultLogger(ctx);

    if (workflow.runResult !== undefined) {
      throw new Error(`Workflow not running: ${args.workflowId}`);
    }
    const maxEntry = await ctx.db
      .query("steps")
      .withIndex("workflow", (q) => q.eq("workflowId", workflow._id))
      .order("desc")
      .first();
    const stepNumberBase = maxEntry ? maxEntry.stepNumber + 1 : 0;
    const workpool = await getWorkpool(ctx, args.workpoolOptions);
    const onComplete = internal.pool.onComplete;

    const entries = await Promise.all(
      args.steps.map(async (stepArgs, index) => {
        const { retry, schedulerOptions } = stepArgs;
        const stepNumber = stepNumberBase + index;
        const stepId = await ctx.db.insert("steps", {
          workflowId: workflow._id,
          stepNumber,
          step: stepArgs.step,
        });
        let entry = await ctx.db.get(stepId);
        assert(entry, "Step not found");
        const step = entry.step;
        const { name } = step;
        console.event("started", {
          workflowId: workflow._id,
          workflowName: workflow.name,
          stepName: name,
          stepNumber,
        });
        if (step.kind === "event") {
          // Note: This modifies entry in place as well.
          entry = await awaitEvent(ctx, entry, {
            name,
            eventId: step.args.eventId,
          });
          if (step.runResult) {
            console.event("eventConsumed", {
              workflowId: entry.workflowId,
              workflowName: workflow.name,
              status: step.runResult.kind,
              eventName: step.name,
              stepNumber: stepNumber,
              durationMs: step.completedAt! - step.startedAt,
            });
          }
        } else if (step.kind === "workflow") {
          const workflowId = await createHandler(ctx, {
            workflowName: step.name,
            workflowHandle: step.handle,
            workflowArgs: step.args,
            maxParallelism: args.workpoolOptions?.maxParallelism,
            onComplete: {
              fnHandle: await createFunctionHandle(
                internal.pool.nestedWorkflowOnComplete,
              ),
              context: {
                stepId,
                generationNumber,
                workpoolOptions: args.workpoolOptions,
              } satisfies OnCompleteContext,
            },
            startAsync: true,
          });
          step.workflowId = workflowId;
        } else if (step.runResult) {
          // Already completed inline by the caller — nothing to enqueue.
          console.event("stepCompleted", {
            workflowId: entry.workflowId,
            workflowName: workflow.name,
            status: step.runResult.kind,
            stepName: step.name,
            stepNumber: stepNumber,
          });
        } else if (step.kind === "sleep") {
          const context: OnCompleteContext = {
            generationNumber,
            stepId,
            workpoolOptions: args.workpoolOptions,
          };
          step.workId = await workpool.enqueueQuery(
            ctx,
            internal.workflow.sleep,
            {},
            { context, onComplete, name, ...schedulerOptions },
          );
        } else if (step.kind === "race") {
          const raceId = entry._id;
          const sent = await sentEventsByNames(ctx, workflow._id, step.events);

          const eventsToWait = new Set(step.events.map((e) => e.name));
          let eventsToConsume: SentEvent[] = [];
          let winner: SentEvent | undefined;
          switch (step.failure) {
            case "discard": {
              const toConsume = new Map<string, SentEvent>();
              for (const s of sent) {
                if (toConsume.has(s.name)) {
                  continue;
                }
                toConsume.set(s.name, s);
                eventsToWait.delete(s.name);
                if (s.state.result.kind === "success") {
                  winner = s;
                  break;
                }
              }
              eventsToConsume = Array.from(toConsume.values());
              break;
            }
            case "retry": {
              const winnerIndex = sent.findIndex(
                (s) => s.state.result.kind === "success",
              );
              if (winnerIndex >= 0) {
                winner = sent[winnerIndex];
                eventsToConsume = sent.slice(0, winnerIndex + 1);
              } else {
                eventsToConsume = sent;
              }
              break;
            }
            case "fail":
            case undefined:
            default: {
              if (sent.length > 0) {
                winner = sent[0];
                eventsToWait.delete(winner.name);
                eventsToConsume.push(winner);
              }
              break;
            }
          }

          if (eventsToWait.size === 0 || winner) {
            if (winner) {
              step.raceWinnerEventId = winner._id;
              step.runResult =
                winner.state.result.kind === "success"
                  ? {
                      kind: "success",
                      returnValue: {
                        eventName: winner.name,
                        value: winner.state.result.returnValue,
                      },
                    }
                  : {
                      kind: "failed",
                      error: runResultToError(winner.state.result),
                    };
              eventsToWait.clear();
            } else {
              step.runResult = {
                kind: "failed",
                error: "Exhausted all events",
              };
            }
            entry.step.inProgress = false;
            entry.step.completedAt = Date.now();
            console.event("stepCompleted", {
              workflowId: entry.workflowId,
              workflowName: workflow.name,
              status: entry.step.runResult!.kind,
              stepName: entry.step.name,
              stepNumber,
            });
          }

          if (step.timeout && entry.step.inProgress) {
            const workId = await workpool.enqueueMutation(
              ctx,
              internal.eventWait.timeout,
              {
                stepId: raceId,
                workpoolOptions: args.workpoolOptions,
                generationNumber,
              },
              {
                runAfter: step.timeout.ms,
              },
            );
            step.timeout.workId = workId;
          }

          await Promise.all([
            ...eventsToConsume.map((e) =>
              ctx.db.patch("events", e._id, {
                state: {
                  kind: "consumed",
                  stepId: raceId,
                  sentAt: e.state.sentAt,
                  waitingAt: Date.now(),
                  consumedAt: Date.now(),
                },
              }),
            ),
            ...Array.from(eventsToWait.values()).map((e) =>
              ctx.db.insert("events", {
                workflowId: workflow._id,
                name: e,
                state: {
                  kind: "waiting",
                  waitingAt: Date.now(),
                  stepId: raceId,
                },
              }),
            ),
          ]);
        } else if (step.kind === "all") {
          const sent = await sentEventsByNames(ctx, workflow._id, step.events);
          const earliestByName = new Map<string, SentEvent>();
          for (const sentEvent of sent) {
            if (earliestByName.has(sentEvent.name)) {
              continue;
            }
            earliestByName.set(sentEvent.name, sentEvent);
          }
          const earliestByNameArray = Array.from(earliestByName.values());
          const firstErrorIndex = earliestByNameArray.findIndex(
            (s) => !!s && s.state.result.kind !== "success",
          );
          const firstError =
            firstErrorIndex >= 0
              ? (earliestByNameArray[firstErrorIndex] as SentEvent & {
                  state: {
                    result: { kind: "failed" | "canceled" };
                  };
                })
              : undefined;
          let eventsToConsume;
          if (firstError) {
            step.runResult = {
              kind: "failed",
              error: runResultToError(firstError.state.result),
            };
            entry.step.inProgress = false;
            entry.step.completedAt = Date.now();
            console.event("stepCompleted", {
              workflowId: entry.workflowId,
              workflowName: workflow.name,
              status: entry.step.runResult!.kind,
              stepName: entry.step.name,
              stepNumber,
            });
            eventsToConsume = new Map(
              earliestByNameArray
                .slice(0, firstErrorIndex + 1)
                .map((s) => [s.name, s]),
            );
          } else {
            eventsToConsume = earliestByName;
          }

          step.fulfilled = step.events
            .map((eventSpec) => {
              const sentEvent = eventsToConsume.get(eventSpec.name);
              if (!sentEvent || sentEvent.state.result.kind !== "success") {
                return null;
              }
              return {
                name: eventSpec.name,
                value: sentEvent.state.result.returnValue,
              };
            })
            .filter((item) => item !== null);

          const fulfilledByName = new Map(
            step.fulfilled.map((item) => [item.name, item.value]),
          );

          let eventsToWait = step.events.filter(
            (e) => !eventsToConsume.has(e.name),
          );

          if (eventsToWait.length === 0 && !step.runResult) {
            step.runResult = {
              kind: "success",
              returnValue: step.events.map((expected) => {
                assert(
                  fulfilledByName.has(expected.name),
                  `Missing fulfilled event ${expected.name}`,
                );
                return fulfilledByName.get(expected.name);
              }),
            };
            entry.step.inProgress = false;
            entry.step.completedAt = Date.now();
            console.event("stepCompleted", {
              workflowId: entry.workflowId,
              workflowName: workflow.name,
              status: entry.step.runResult!.kind,
              stepName: entry.step.name,
              stepNumber,
            });
          }

          if (!entry.step.inProgress) {
            eventsToWait = [];
          }

          if (step.timeout && entry.step.inProgress) {
            const workId = await workpool.enqueueMutation(
              ctx,
              internal.eventWait.timeout,
              {
                stepId,
                workpoolOptions: args.workpoolOptions,
                generationNumber,
              },
              {
                runAfter: step.timeout.ms,
              },
            );
            step.timeout.workId = workId;
          }

          await Promise.all([
            ...Array.from(eventsToConsume.values()).map((sentEvent) =>
              ctx.db.patch("events", sentEvent._id, {
                state: {
                  kind: "consumed",
                  stepId,
                  sentAt: sentEvent.state.sentAt,
                  waitingAt: Date.now(),
                  consumedAt: Date.now(),
                },
              }),
            ),
            ...eventsToWait.map((eventSpec) =>
              ctx.db.insert("events", {
                workflowId: workflow._id,
                name: eventSpec.name,
                state: {
                  kind: "waiting",
                  waitingAt: Date.now(),
                  stepId,
                },
              }),
            ),
          ]);
        } else if (step.kind === "allSettled") {
          const sent = await sentEventsByNames(ctx, workflow._id, step.events);
          const earliestByName = new Map<string, SentEvent>();
          for (const sentEvent of sent) {
            if (earliestByName.has(sentEvent.name)) {
              continue;
            }
            earliestByName.set(sentEvent.name, sentEvent);
          }

          step.settled = step.events
            .map((eventSpec) => {
              const sentEvent = earliestByName.get(eventSpec.name);
              if (!sentEvent) {
                return null;
              }
              return {
                name: eventSpec.name,
                result:
                  sentEvent.state.result.kind === "success"
                    ? {
                        status: "fulfilled" as const,
                        value: sentEvent.state.result.returnValue,
                      }
                    : {
                        status: "rejected" as const,
                        reason: runResultToError(sentEvent.state.result),
                      },
              };
            })
            .filter((item) => item !== null);

          const settledByName = new Map(
            step.settled.map((item) => [item.name, item.result]),
          );

          let eventsToWait = step.events.filter(
            (e) => !earliestByName.has(e.name),
          );

          if (eventsToWait.length === 0) {
            step.runResult = {
              kind: "success",
              returnValue: step.events.map((expected) => {
                const result = settledByName.get(expected.name);
                assert(result, `Missing settled event ${expected.name}`);
                return {
                  name: expected.name,
                  ...result,
                };
              }),
            };
            entry.step.inProgress = false;
            entry.step.completedAt = Date.now();
            console.event("stepCompleted", {
              workflowId: entry.workflowId,
              workflowName: workflow.name,
              status: entry.step.runResult!.kind,
              stepName: entry.step.name,
              stepNumber,
            });
          }

          if (!entry.step.inProgress) {
            eventsToWait = [];
          }

          if (step.timeout && entry.step.inProgress) {
            const workId = await workpool.enqueueMutation(
              ctx,
              internal.eventWait.timeout,
              {
                stepId,
                workpoolOptions: args.workpoolOptions,
                generationNumber,
              },
              {
                runAfter: step.timeout.ms,
              },
            );
            step.timeout.workId = workId;
          }

          await Promise.all([
            ...Array.from(earliestByName.values()).map((sentEvent) =>
              ctx.db.patch("events", sentEvent._id, {
                state: {
                  kind: "consumed",
                  stepId,
                  sentAt: sentEvent.state.sentAt,
                  waitingAt: Date.now(),
                  consumedAt: Date.now(),
                },
              }),
            ),
            ...eventsToWait.map((eventSpec) =>
              ctx.db.insert("events", {
                workflowId: workflow._id,
                name: eventSpec.name,
                state: {
                  kind: "waiting",
                  waitingAt: Date.now(),
                  stepId,
                },
              }),
            ),
          ]);
        } else {
          const context: OnCompleteContext = {
            generationNumber,
            stepId,
            workpoolOptions: args.workpoolOptions,
          };
          let workId: WorkId;
          switch (step.functionType) {
            case "query": {
              workId = await workpool.enqueueQuery(
                ctx,
                step.handle as FunctionHandle<"query">,
                step.args,
                { context, onComplete, name, ...schedulerOptions },
              );
              break;
            }
            case "mutation": {
              workId = await workpool.enqueueMutation(
                ctx,
                step.handle as FunctionHandle<"mutation">,
                step.args,
                { context, onComplete, name, ...schedulerOptions },
              );
              break;
            }
            case "action": {
              workId = await workpool.enqueueAction(
                ctx,
                step.handle as FunctionHandle<"action">,
                step.args,
                { context, onComplete, name, retry, ...schedulerOptions },
              );
              break;
            }
          }
          step.workId = workId;
        }
        await ctx.db.replace(entry._id, entry);

        return entry;
      }),
    );
    return entries;
  },
});
