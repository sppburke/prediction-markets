"use client";

// Sign-out control (#398 WS3). Uses next-auth's client `signOut()` rather than a raw link to the
// API route (which trips next/no-html-link-for-pages).
import { signOut } from "next-auth/react";

export function SignOut() {
  return (
    <button type="button" onClick={() => signOut()} className="hover:text-accent">
      sign out
    </button>
  );
}
