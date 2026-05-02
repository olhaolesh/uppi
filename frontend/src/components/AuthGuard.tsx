import type { PropsWithChildren } from "react";
import { Navigate, useLocation } from "react-router-dom";

import { useAuth } from "../auth/AuthContext";

export default function AuthGuard({ children }: PropsWithChildren) {
  const { phase } = useAuth();
  const location = useLocation();

  if (phase === "loading") {
    return (
      <div className="screen-center">
        <div className="loading-card">
          <p className="eyebrow">UPPI Web Shell</p>
          <h1>Перевіряю веб-сесію</h1>
          <p>Frontend skeleton підтягує тільки `/auth/me` і не запускає бізнес-flow.</p>
        </div>
      </div>
    );
  }

  if (phase !== "authenticated") {
    return (
      <Navigate
        to="/login"
        replace
        state={{
          from: `${location.pathname}${location.search}${location.hash}`,
        }}
      />
    );
  }

  return <>{children}</>;
}
