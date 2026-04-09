type StatusBlockProps = {
  status: "idle" | "loading" | "error";
  message?: string;
};

export function StatusBlock({ status, message }: StatusBlockProps) {
  return (
    <div className={`status-block status-${status}`}>
      <p>{message ?? (status === "loading" ? "Loading data…" : "No data selected.")}</p>
    </div>
  );
}
