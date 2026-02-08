import { createErrorFromThrown } from "../errors";
import type { ClientHooks } from "../types/index";

const SENSITIVE_KEYS = new Set([
  "password",
  "pass",
  "pwd",
  "secret",
  "token",
  "access_token",
  "refresh_token",
  "apiKey",
  "apikey",
  "api_key",
]);

function looksLikeJWT(s: string): boolean {
  // simple heuristic: three base64url parts separated by dots
  return /^[-A-Za-z0-9_=]+\.[-A-Za-z0-9_=]+\.[-A-Za-z0-9_=]+$/.test(s);
}

function redactValue(v: unknown): unknown {
  if (typeof v === "string") {
    if (v.length > 200) return "[REDACTED]";
    if (looksLikeJWT(v)) return "[REDACTED]";
    return v;
  }
  if (Array.isArray(v)) return v.map(redactValue);
  if (v && typeof v === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
      if (SENSITIVE_KEYS.has(k)) {
        out[k] = "[REDACTED]";
      } else {
        out[k] = redactValue(val);
      }
    }
    return out;
  }
  return v;
}

export function redactParams(params: unknown[]): unknown[] {
  return params.map(redactValue);
}

/**
 * Safely call `onQuery` asynchronously so hook errors cannot break core logic.
 * Redacts likely-sensitive params before invoking.
 */
export function safeInvokeOnQuery(
  hooks: ClientHooks | undefined,
  text: string,
  params: unknown[],
): void {
  if (!hooks?.onQuery) return;
  const safeParams = redactParams(params || []);
  setImmediate(() => {
    try {
      hooks.onQuery?.(text, safeParams as unknown[]);
    } catch (err) {
      try {
        hooks.onError?.(createErrorFromThrown("HOOK", "onQuery hook failed", err));
      } catch {
        // swallow
      }
    }
  });
}

export function safeInvokeOnError(hooks: ClientHooks | undefined, err: unknown): void {
  if (!hooks?.onError) return;
  setImmediate(() => {
    try {
      // If err looks like DbError, sanitize details before invoking hook to avoid leaking raw DB internals.
      const e = err as any;
      if (e && typeof e === "object" && typeof e.code === "string" && typeof e.message === "string") {
        const safe: Record<string, unknown> = { code: e.code, message: e.message };
        if (typeof e.pgCode === "string") safe.pgCode = e.pgCode;
        // If details is an object, copy only safe fields
        const d = e.details;
        if (d && typeof d === "object") {
          const safeDetails: Record<string, unknown> = {};
          if (typeof (d as any).code === "string") safeDetails.code = (d as any).code;
          if (typeof (d as any).detail === "string") safeDetails.detail = (d as any).detail;
          if (Object.keys(safeDetails).length > 0) safe.details = safeDetails;
        }
        hooks.onError?.(safe as any);
      } else {
        hooks.onError?.(err as any);
      }
    } catch {
      // swallow to avoid throwing during error reporting
    }
  });
}
