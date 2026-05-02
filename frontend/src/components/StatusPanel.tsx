import type { ReactNode } from "react";

type StatusPanelTone = "info" | "warning" | "success";

type StatusPanelProps = {
  title: string;
  tone?: StatusPanelTone;
  children: ReactNode;
};

export default function StatusPanel({
  title,
  tone = "info",
  children,
}: StatusPanelProps) {
  return (
    <section className={`status-panel status-panel--${tone}`}>
      <div className="status-panel__header">
        <p className="eyebrow">Статус</p>
        <h3>{title}</h3>
      </div>
      <div className="status-panel__body">{children}</div>
    </section>
  );
}
