export function Panel({
  title,
  children,
  className = "",
}: {
  title?: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <section
      className={`rounded-lg border border-border bg-panel p-4 ${className}`}
    >
      {title ? (
        <h2 className="mb-3 text-xs font-semibold uppercase tracking-wider text-muted">
          {title}
        </h2>
      ) : null}
      {children}
    </section>
  );
}

export function StateNotice({
  kind,
  message,
}: {
  kind: "loading" | "error" | "empty";
  message: string;
}) {
  const tone =
    kind === "error" ? "text-neg" : kind === "empty" ? "text-muted" : "text-accent";
  return (
    <div className="rounded-lg border border-border bg-panel p-8 text-center">
      <p className={`text-sm ${tone}`}>{message}</p>
    </div>
  );
}
