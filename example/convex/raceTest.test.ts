/// <reference types="vite/client" />

import { expect, describe, test, vi, beforeEach, afterEach } from "vitest";
import { initConvexTest } from "./setup.test";
import { internal } from "./_generated/api";
import { workflow } from "./raceTest";
import { assert } from "convex-helpers";

describe("raceEvents", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  test("basic race", async () => {
    const t = initConvexTest();
    const workflowId = await t.run((ctx) =>
      workflow.start(ctx, internal.raceTest.basicRace, {}),
    );
    await t.mutation(internal.raceTest.sendEventA, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    const status = await t.query(internal.inlineTest.checkStatus, {
      workflowId,
    });
    expect(status.type).toBe("completed");
    assert(status.type === "completed");
    expect(status.result).toBe("eventA");
  });

  test("multiple pre-existing events - earliest-sent wins", async () => {
    const t = initConvexTest();
    const workflowId = await t.run((ctx) =>
      workflow.start(ctx, internal.raceTest.raceWithExtraStep, {}),
    );
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    await t.mutation(internal.raceTest.sendEventB, { workflowId });
    await t.mutation(internal.raceTest.sendEventA, { workflowId });
    await t.mutation(internal.raceTest.sendBlock, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    const status = await t.query(internal.inlineTest.checkStatus, {
      workflowId,
    });
    expect(status.type).toBe("completed");
    assert(status.type === "completed");
    expect(status.result).toBe("eventB");
  });

  test("timeout triggers failure", async () => {
    const t = initConvexTest();
    const workflowId = await t.run((ctx) =>
      workflow.start(ctx, internal.raceTest.raceWithTimeout, { timeout: 1000 }),
    );
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    const status = await t.query(internal.inlineTest.checkStatus, {
      workflowId,
    });
    expect(status.type).toBe("failed");
    assert(status.type === "failed");
    expect(status.error).toContain("Timeout");
  });

  test("event arrives before timeout", async () => {
    const t = initConvexTest();
    const workflowId = await t.run((ctx) =>
      workflow.start(ctx, internal.raceTest.raceWithTimeout, {
        timeout: 60000,
      }),
    );
    await t.mutation(internal.raceTest.sendEventA, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    const status = await t.query(internal.inlineTest.checkStatus, {
      workflowId,
    });
    expect(status.type).toBe("completed");
    assert(status.type === "completed");
    expect(status.result).toBe("eventA");
  });

  test("validators parse correctly", async () => {
    const t = initConvexTest();
    const workflowId = await t.run((ctx) =>
      workflow.start(ctx, internal.raceTest.raceWithValidators, {}),
    );
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    await t.mutation(internal.raceTest.sendApproval, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    const status = await t.query(internal.inlineTest.checkStatus, {
      workflowId,
    });
    expect(status.type).toBe("completed");
    assert(status.type === "completed");
    expect(status.result).toEqual({
      name: "approval",
      value: { proposal: "A" },
    });
  });
});

describe("failure modes", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  test("failure mode fail - failure propagates", async () => {
    const t = initConvexTest();
    const workflowId = await t.run((ctx) =>
      workflow.start(ctx, internal.raceTest.raceWithFailureFail, {}),
    );
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    await t.mutation(internal.raceTest.sendFailedEventA, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    const status = await t.query(internal.inlineTest.checkStatus, {
      workflowId,
    });
    expect(status.type).toBe("failed");
    assert(status.type === "failed");
    expect(status.error).toContain("intentional failure");
  });

  test("failure mode fail - success event wins immediately", async () => {
    const t = initConvexTest();
    const workflowId = await t.run((ctx) =>
      workflow.start(ctx, internal.raceTest.raceWithFailureFail, {}),
    );
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    await t.mutation(internal.raceTest.sendEventA, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    const status = await t.query(internal.inlineTest.checkStatus, {
      workflowId,
    });
    expect(status.type).toBe("completed");
    assert(status.type === "completed");
    expect(status.result).toBe("eventA");
  });

  test("failure mode retry - retries on failure", async () => {
    const t = initConvexTest();
    const workflowId = await t.run((ctx) =>
      workflow.start(ctx, internal.raceTest.raceWithFailureRetry, {}),
    );
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    await t.mutation(internal.raceTest.sendFailedEventA, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    const status = await t.query(internal.inlineTest.checkStatus, {
      workflowId,
    });
    expect(status.type).toBe("inProgress");
    await t.mutation(internal.raceTest.sendEventA, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    const statusAfter = await t.query(internal.inlineTest.checkStatus, {
      workflowId,
    });
    expect(statusAfter.type).toBe("completed");
    assert(statusAfter.type === "completed");
    expect(statusAfter.result).toBe("eventA");
  });

  test("failure mode discard - ignores failures", async () => {
    const t = initConvexTest();
    const workflowId = await t.run((ctx) =>
      workflow.start(ctx, internal.raceTest.raceWithFailureDiscard, {}),
    );
    await t.mutation(internal.raceTest.sendFailedEventB, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    await t.mutation(internal.raceTest.sendEventB, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    const statusBefore = await t.query(internal.inlineTest.checkStatus, {
      workflowId,
    });
    expect(statusBefore.type).toBe("inProgress");
    await t.mutation(internal.raceTest.sendEventA, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    const statusAfter = await t.query(internal.inlineTest.checkStatus, {
      workflowId,
    });
    expect(statusAfter.type).toBe("completed");
    assert(statusAfter.type === "completed");
    expect(statusAfter.result).toBe("eventA");
  });

  test("failure mode discard - exhausted all events", async () => {
    const t = initConvexTest();
    const workflowId = await t.run((ctx) =>
      workflow.start(ctx, internal.raceTest.raceWithFailureDiscard, {}),
    );
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    await t.mutation(internal.raceTest.sendFailedEventA, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    await t.mutation(internal.raceTest.sendFailedEventB, { workflowId });
    await t.finishAllScheduledFunctions(vi.runAllTimers);
    const status = await t.query(internal.inlineTest.checkStatus, {
      workflowId,
    });
    expect(status.type).toBe("failed");
    assert(status.type === "failed");
    expect(status.error).toContain("Exhausted all events");
  });
});
