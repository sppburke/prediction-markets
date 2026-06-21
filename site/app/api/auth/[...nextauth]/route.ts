// next-auth v5 route handler (#398 WS3, step 15). The GCP OAuth redirect URI is
// `https://<domain>/api/auth/callback/google` (step 21), served by these handlers.
import { handlers } from "@/auth";

export const { GET, POST } = handlers;
