import { v } from "convex/values";
import { internalMutation } from "./_generated/server.js";
import { assert } from "convex-helpers";
import { enqueueWorkflow, getWorkpool, workpoolOptions } from "./pool.js";
import { getDefaultLogger } from "./utils.js";

function isSupportedStepKind(kind: string | undefined) {
  return kind === "race" || kind === "all" || kind === "allSettled";
}

export const timeout = internalMutation({
  args: {
    stepId: v.id("steps"),
    workpoolOptions: workpoolOptions,
    generationNumber: v.number(),
  },
  async handler(ctx, args) {
    const console = await getDefaultLogger(ctx);
    const step = await ctx.db.get("steps", args.stepId);
    assert(step, `Step not found: ${args.stepId}`);
    assert(
      isSupportedStepKind(step.step.kind),
      `Step is not an event wait: ${args.stepId}`,
    );
    if (!step.step.inProgress) {
      console.error(`Step ${args.stepId} is not in progress`);
      return;
    }
    const workflow = await ctx.db.get("workflows", step.workflowId);
    assert(workflow, `Workflow ${step.workflowId} not found`);
    if (workflow.generationNumber !== args.generationNumber) {
      console.error(
        `Workflow: ${step.workflowId} already has generation number ${workflow.generationNumber} when completing ${args.stepId}. Expected ${args.generationNumber}`,
      );
      return;
    }

    step.step.runResult = { kind: "failed", error: "Timeout" };
    step.step.inProgress = false;
    step.step.completedAt = Date.now();
    await ctx.db.replace("steps", step._id, step);

    const waitingEvents = await ctx.db
      .query("events")
      .withIndex("workflowId_state", (q) =>
        q.eq("workflowId", step.workflowId).eq("state.kind", "waiting"),
      )
      .filter((q) => q.eq(q.field("state.stepId"), args.stepId))
      .collect();
    await Promise.all(
      waitingEvents.map((event) => ctx.db.delete("events", event._id)),
    );

    const workpool = await getWorkpool(ctx, args.workpoolOptions);
    await enqueueWorkflow(ctx, workflow, workpool);
  },
});
