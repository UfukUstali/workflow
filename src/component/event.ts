// Get event status

import { v } from "convex/values";
import { mutation, type MutationCtx } from "./_generated/server.js";
import { vResultValidator } from "@convex-dev/workpool";
import type { Doc, Id } from "./_generated/dataModel.js";
import { assert } from "convex-helpers";
import { enqueueWorkflow, getWorkpool, workpoolOptions } from "./pool.js";
import { runResultToError } from "./utils.js";

async function completeEventWaitStep(
  ctx: MutationCtx,
  step: Doc<"steps">,
  workflowId: Id<"workflows">,
  options?: Parameters<typeof getWorkpool>[1],
) {
  if (
    (step.step.kind === "race" ||
      step.step.kind === "all" ||
      step.step.kind === "allSettled") &&
    step.step.timeout?.workId
  ) {
    const workpool = await getWorkpool(ctx, {});
    await workpool.cancel(ctx, step.step.timeout.workId);
  }
  step.step.inProgress = false;
  step.step.completedAt = Date.now();
  await ctx.db.replace("steps", step._id, step);
  const waitingEvents = await ctx.db
    .query("events")
    .withIndex("workflowId_state", (q) =>
      q.eq("workflowId", workflowId).eq("state.kind", "waiting"),
    )
    .filter((q) => q.eq(q.field("state.stepId"), step._id))
    .collect();
  await Promise.all(
    waitingEvents.map((waitingEvent) =>
      ctx.db.delete("events", waitingEvent._id),
    ),
  );
  const workflow = await ctx.db.get("workflows", workflowId);
  assert(workflow, `Workflow ${workflowId} not found`);
  const workpool = await getWorkpool(ctx, options);
  await enqueueWorkflow(ctx, workflow, workpool);
}

export async function awaitEvent(
  ctx: MutationCtx,
  entry: Doc<"steps">,
  args: { eventId?: Id<"events">; name: string },
) {
  const event = await getOrCreateEvent(ctx, entry.workflowId, args, [
    "sent",
    "created",
  ]);
  switch (event.state.kind) {
    case "consumed": {
      throw new Error(
        `Event already consumed: ${event._id} (${entry.step.name}) in workflow ${entry.workflowId} step ${entry.stepNumber} (${entry._id})`,
      );
    }
    case "waiting": {
      throw new Error(
        `Event already waiting: ${event._id} (${entry.step.name}) in workflow ${entry.workflowId} step ${entry.stepNumber} (${entry._id})`,
      );
    }
  }

  switch (event.state.kind) {
    case "sent": {
      await ctx.db.patch(event._id, {
        state: {
          kind: "consumed",
          sentAt: event.state.sentAt,
          waitingAt: Date.now(),
          consumedAt: Date.now(),
          stepId: entry._id,
        },
      });
      entry.step.runResult = event.state.result;
      entry.step.inProgress = false;
      entry.step.completedAt = Date.now();
      break;
    }
    case "created": {
      await ctx.db.patch(event._id, {
        state: {
          kind: "waiting",
          waitingAt: Date.now(),
          stepId: entry._id,
        },
      });
      break;
    }
  }
  assert(entry.step.kind === "event", "Step is not an event");
  entry.step.eventId = event._id;
  // if there's a name, see if there's one to consume.
  // if it's there, mark it consumed and swap in the result.
  return entry;
}

async function getOrCreateEvent(
  ctx: MutationCtx,
  workflowId: Id<"workflows"> | undefined,
  args: { eventId?: Id<"events">; name?: string },
  statuses: Doc<"events">["state"]["kind"][],
): Promise<Doc<"events">> {
  if (args.eventId) {
    const event = await ctx.db.get(args.eventId);
    if (!event) {
      throw new Error(
        `Event not found: ${args.eventId} (${args.name}) in workflow ${workflowId}`,
      );
    }
    return event;
  }
  assert(args.name, "Name is required if eventId is not specified");
  assert(workflowId, "workflowId is required if eventId is not specified");
  for (const status of statuses) {
    const event = await ctx.db
      .query("events")
      .withIndex("workflowId_state", (q) =>
        q.eq("workflowId", workflowId).eq("state.kind", status),
      )
      .filter((q) => q.eq(q.field("name"), args.name))
      .first();
    if (event) return event;
  }
  const eventId = await ctx.db.insert("events", {
    workflowId,
    name: args.name,
    state: {
      kind: "created",
    },
  });
  return (await ctx.db.get(eventId))!;
}

export const send = mutation({
  args: {
    workflowId: v.optional(v.id("workflows")),
    eventId: v.optional(v.id("events")),
    name: v.optional(v.string()),
    result: vResultValidator,
    workpoolOptions: v.optional(workpoolOptions),
  },
  returns: v.id("events"),
  handler: async (ctx, args) => {
    const event = await getOrCreateEvent(
      ctx,
      args.workflowId,
      {
        eventId: args.eventId,
        name: args.name,
      },
      ["waiting", "created"],
    );
    const { workflowId } = event;
    switch (event.state.kind) {
      case "sent": {
        throw new Error(
          `Event already sent: ${event._id} (${event.name}) in workflow ${workflowId}`,
        );
      }
      case "consumed": {
        throw new Error(
          `Event already consumed: ${event._id} (${event.name}) in workflow ${workflowId}`,
        );
      }
      case "created": {
        await ctx.db.patch(event._id, {
          state: { kind: "sent", result: args.result, sentAt: Date.now() },
        });
        break;
      }
      case "waiting": {
        const step = await ctx.db.get(event.state.stepId);
        assert(
          step,
          `Entry ${event.state.stepId} not found when sending event ${event._id} (${event.name}) in workflow ${workflowId}`,
        );
        assert(
          step.step.kind === "event" ||
            step.step.kind === "race" ||
            step.step.kind === "all" ||
            step.step.kind === "allSettled",
          "Step is not an event",
        );
        if (step.step.kind === "event") {
          step.step.eventId = event._id;
          step.step.runResult = args.result;
          step.step.inProgress = false;
          step.step.completedAt = Date.now();
          await ctx.db.replace(step._id, step);
          await ctx.db.patch(event._id, {
            state: {
              kind: "consumed",
              stepId: step._id,
              waitingAt: event.state.waitingAt,
              sentAt: Date.now(),
              consumedAt: Date.now(),
            },
          });
          const anyMoreEvents = await ctx.db
            .query("events")
            .withIndex("workflowId_state", (q) =>
              q.eq("workflowId", workflowId).eq("state.kind", "waiting"),
            )
            .order("desc")
            .first();
          if (!anyMoreEvents) {
            const workflow = await ctx.db.get(workflowId);
            assert(workflow, `Workflow ${workflowId} not found`);
            const workpool = await getWorkpool(ctx, args.workpoolOptions);
            await enqueueWorkflow(ctx, workflow, workpool);
          }
        } else if (step.step.kind === "race") {
          if (args.result.kind !== "success" && step.step.failure === "retry") {
            break;
          }
          await ctx.db.patch("events", event._id, {
            state: {
              kind: "consumed",
              stepId: step._id,
              waitingAt: event.state.waitingAt,
              sentAt: Date.now(),
              consumedAt: Date.now(),
            },
          });
          const losingEvents = await ctx.db
            .query("events")
            .withIndex("workflowId_state", (q) =>
              q.eq("workflowId", workflowId).eq("state.kind", "waiting"),
            )
            .filter((q) => q.eq(q.field("state.stepId"), step._id))
            .collect();
          // until all events are exhausted continue waiting
          if (
            args.result.kind !== "success" &&
            step.step.failure === "discard" &&
            losingEvents.length > 0
          ) {
            break;
          }
          if (step.step.timeout?.workId) {
            const workpool = await getWorkpool(ctx, {});
            await workpool.cancel(ctx, step.step.timeout.workId);
          }
          if (args.result.kind === "success") {
            step.step.raceWinnerEventId = event._id;
            step.step.runResult = {
              kind: "success",
              returnValue: {
                eventName: event.name,
                value: args.result.returnValue,
              },
            };
          } else {
            switch (step.step.failure) {
              case "discard": {
                step.step.runResult = {
                  kind: "failed",
                  error: "Exhausted all events",
                };
                break;
              }
              case "retry": {
                // IMPOSSIBLE TO REACH HERE
                break;
              }
              case "fail":
              case undefined: {
                step.step.raceWinnerEventId = event._id;
                step.step.runResult = {
                  kind: "failed",
                  error: runResultToError(args.result),
                };
                break;
              }
            }
          }
          step.step.inProgress = false;
          step.step.completedAt = Date.now();
          await ctx.db.replace("steps", step._id, step);
          await Promise.all(
            losingEvents.map((losing) => ctx.db.delete("events", losing._id)),
          );
          const workflow = await ctx.db.get("workflows", workflowId);
          assert(workflow, `Workflow ${workflowId} not found`);
          const workpool = await getWorkpool(ctx, args.workpoolOptions);
          await enqueueWorkflow(ctx, workflow, workpool);
        } else if (step.step.kind === "all") {
          const eventWaitStep = step.step;
          await ctx.db.patch("events", event._id, {
            state: {
              kind: "consumed",
              stepId: step._id,
              waitingAt: event.state.waitingAt,
              sentAt: Date.now(),
              consumedAt: Date.now(),
            },
          });

          if (args.result.kind !== "success") {
            eventWaitStep.runResult = {
              kind: "failed",
              error: runResultToError(args.result),
            };
            await completeEventWaitStep(
              ctx,
              step,
              workflowId,
              args.workpoolOptions,
            );
            break;
          }

          eventWaitStep.fulfilled = [
            ...eventWaitStep.fulfilled.filter((f) => f.name !== event.name),
            {
              name: event.name,
              value: args.result.returnValue,
            },
          ];

          if (eventWaitStep.fulfilled.length < eventWaitStep.events.length) {
            await ctx.db.replace("steps", step._id, step);
            break;
          }

          const fulfilledByName = new Map(
            eventWaitStep.fulfilled.map((f) => [f.name, f.value]),
          );
          eventWaitStep.runResult = {
            kind: "success",
            returnValue: eventWaitStep.events.map((expected) => {
              assert(
                fulfilledByName.has(expected.name),
                `Missing fulfilled event ${expected.name}`,
              );
              return fulfilledByName.get(expected.name);
            }),
          };
          await completeEventWaitStep(
            ctx,
            step,
            workflowId,
            args.workpoolOptions,
          );
        } else {
          assert(step.step.kind === "allSettled", "Step is not allSettled");
          const eventWaitStep = step.step;
          await ctx.db.patch("events", event._id, {
            state: {
              kind: "consumed",
              stepId: step._id,
              waitingAt: event.state.waitingAt,
              sentAt: Date.now(),
              consumedAt: Date.now(),
            },
          });

          eventWaitStep.settled = [
            ...eventWaitStep.settled.filter((s) => s.name !== event.name),
            {
              name: event.name,
              result:
                args.result.kind === "success"
                  ? { status: "fulfilled", value: args.result.returnValue }
                  : {
                      status: "rejected",
                      reason: runResultToError(args.result),
                    },
            },
          ];

          if (eventWaitStep.settled.length < eventWaitStep.events.length) {
            await ctx.db.replace("steps", step._id, step);
            break;
          }

          const settledByName = new Map(
            eventWaitStep.settled.map((s) => [s.name, s.result]),
          );
          eventWaitStep.runResult = {
            kind: "success",
            returnValue: eventWaitStep.events.map((expected) => {
              const settled = settledByName.get(expected.name);
              assert(settled, `Missing settled event ${expected.name}`);
              return {
                name: expected.name,
                ...settled,
              };
            }),
          };
          await completeEventWaitStep(
            ctx,
            step,
            workflowId,
            args.workpoolOptions,
          );
        }
        break;
      }
    }
    return event._id;
  },
});

export const create = mutation({
  args: {
    name: v.string(),
    workflowId: v.id("workflows"),
  },
  returns: v.id("events"),
  handler: async (ctx, args) => {
    const eventId = await ctx.db.insert("events", {
      workflowId: args.workflowId,
      name: args.name,
      state: {
        kind: "created",
      },
    });
    return eventId;
  },
});
