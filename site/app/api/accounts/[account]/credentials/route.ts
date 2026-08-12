// Write-only sealed credential rotation (#508 Phase C, Decision 9). Plaintext exists only long
// enough to encrypt once to PE_AGE_RECIPIENT; responses and metadata reads never expose it.
import { Encrypter, armor } from "age-encryption";
import { NextResponse } from "next/server";

import { auth } from "@/auth";
import { ALLOWED_EMAIL } from "@/lib/auth";
import { resolveAccess } from "@/lib/authz";
import {
  buildCredentialPlaintext,
  buildCredentialRotationArgs,
  nextBundleVersion,
} from "@/lib/credentials";
import { getServiceRoleSupabase } from "@/lib/supabase-server";

export async function POST(
  req: Request,
  { params }: { params: Promise<{ account: string }> },
) {
  const session = await auth();
  const access = await resolveAccess(session?.user?.email);
  if (access?.role !== "admin") {
    return NextResponse.json({ error: "forbidden" }, { status: 403 });
  }

  const recipient = process.env.PE_AGE_RECIPIENT;
  if (!recipient) {
    return NextResponse.json(
      { error: "PE_AGE_RECIPIENT is not configured; credential was not stored" },
      { status: 500 },
    );
  }

  let parsed: unknown;
  try {
    parsed = await req.json();
  } catch {
    return NextResponse.json({ error: "invalid JSON" }, { status: 400 });
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return NextResponse.json({ error: "JSON object required" }, { status: 400 });
  }
  const body = parsed as { key_id?: unknown; credentials?: unknown };
  if (
    typeof body.key_id !== "string" ||
    body.key_id.trim() === "" ||
    typeof body.credentials !== "object" ||
    body.credentials === null ||
    Array.isArray(body.credentials)
  ) {
    return NextResponse.json(
      { error: "non-empty string `key_id` and object `credentials` required" },
      { status: 400 },
    );
  }

  const { account } = await params;
  const supabase = getServiceRoleSupabase();
  const { data: previous, error: readError } = await supabase
    .from("account_credentials")
    .select("bundle_version")
    .eq("account_id", account)
    .maybeSingle();
  if (readError) return NextResponse.json({ error: readError.message }, { status: 500 });

  // The RPC compare-and-sets on exactly current+1 under the account lock (#508 review):
  // a concurrent rotation makes it raise a version-conflict, surfaced below as 409 so
  // the admin re-submits against the fresh state (never two bundles sealing one version).
  const bundleVersion = nextBundleVersion(previous?.bundle_version);
  const keyId = body.key_id.trim();
  const plaintext = buildCredentialPlaintext({
    accountId: account,
    bundleVersion,
    keyId,
    credentials: body.credentials as Record<string, unknown>,
  });

  let sealedBundle: string;
  try {
    const encrypter = new Encrypter();
    encrypter.addRecipient(recipient);
    sealedBundle = armor.encode(await encrypter.encrypt(plaintext));
  } catch {
    return NextResponse.json(
      { error: "credential encryption failed; credential was not stored" },
      { status: 500 },
    );
  }

  const args = buildCredentialRotationArgs({
    accountId: account,
    bundleVersion,
    keyId,
    sealedBundle,
    actor: ALLOWED_EMAIL,
  });
  const { error } = await supabase.rpc("account_rotate_credentials", args);
  if (error) {
    const conflict = error.message.includes("version conflict");
    return NextResponse.json({ error: error.message }, { status: conflict ? 409 : 500 });
  }
  return NextResponse.json({
    ok: true,
    bundle_version: bundleVersion,
    key_id: keyId,
    fingerprint: args.p_fingerprint,
  });
}
