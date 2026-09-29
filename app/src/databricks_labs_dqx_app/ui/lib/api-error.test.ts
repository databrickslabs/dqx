import { describe, expect, test } from "bun:test";
import { extractApiError } from "./api-error";

describe("extractApiError", () => {
  test("returns a string FastAPI error detail", () => {
    const error = { response: { data: { detail: "Warehouse not found." } } };

    expect(extractApiError(error, "Fallback")).toBe("Warehouse not found.");
  });

  test("returns the summary from a structured FastAPI error detail", () => {
    const error = {
      response: {
        data: {
          detail: {
            code: "warehouse_permissions_missing",
            summary: "The app service principal needs CAN_USE on the SQL warehouse.",
            instructions: ["Grant CAN_USE to the app service principal."],
          },
        },
      },
    };

    expect(extractApiError(error, "Couldn't save compute settings.")).toBe(
      "The app service principal needs CAN_USE on the SQL warehouse.",
    );
  });

  test("returns the fallback for an undefined error", () => {
    expect(extractApiError(undefined, "Couldn't save compute settings.")).toBe(
      "Couldn't save compute settings.",
    );
  });

  test("returns the fallback for a null error", () => {
    expect(extractApiError(null, "Couldn't save compute settings.")).toBe(
      "Couldn't save compute settings.",
    );
  });

  test("returns the fallback when a structured detail has no string summary", () => {
    const error = { response: { data: { detail: { code: "conflict", summary: null } } } };

    expect(extractApiError(error, "Fallback")).toBe("Fallback");
  });
});
