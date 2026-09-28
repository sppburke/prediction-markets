export const SERVICE_READY_TIMEOUT_MS = 2_000;

export type ServiceContractObservation = {
  contract: "updated" | "old" | "unverified" | "unreachable";
  ready: boolean | null;
  issues: string[];
};

export function classifyServiceReady(body: unknown): ServiceContractObservation {
  if (typeof body !== "object" || body === null || Array.isArray(body)) {
    return { contract: "unreachable", ready: null, issues: [] };
  }
  const value = body as Record<string, unknown>;
  const ready = typeof value.ready === "boolean" ? value.ready : null;
  const issues = Array.isArray(value.issues) && value.issues.every((issue) => typeof issue === "string")
    ? value.issues as string[]
    : [];
  if (value.live_control_contract === "owner_requested_mode_v1") {
    return { contract: "updated", ready, issues };
  }
  if (ready !== null && (value.issues === undefined || issues === value.issues)) {
    return { contract: "old", ready, issues };
  }
  return { contract: "unverified", ready, issues };
}

export async function readServiceReady(
  configuredUrl: string | undefined,
  request: typeof fetch = fetch,
): Promise<ServiceContractObservation> {
  let url: URL;
  try {
    if (!configuredUrl) throw new Error("service URL absent");
    url = new URL(configuredUrl);
    if (url.protocol !== "http:" || !["127.0.0.1", "localhost", "[::1]"].includes(url.hostname) || url.pathname !== "/health/ready") {
      throw new Error("service readiness URL must be loopback /health/ready");
    }
    const response = await request(url.toString(), {
      cache: "no-store",
      signal: AbortSignal.timeout(SERVICE_READY_TIMEOUT_MS),
    });
    return classifyServiceReady(await response.json());
  } catch {
    return { contract: "unreachable", ready: null, issues: [] };
  }
}
