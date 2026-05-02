import { useState, type FormEvent } from "react";
import { Navigate, useLocation, useNavigate } from "react-router-dom";

import { useAuth } from "../auth/AuthContext";

type LoginLocationState = {
  from?: string;
};

function resolveReturnPath(state: unknown): string {
  if (typeof state !== "object" || state === null || !("from" in state)) {
    return "/attestazioni/generate";
  }
  const candidate = (state as LoginLocationState).from;
  return typeof candidate === "string" && candidate.trim()
    ? candidate
    : "/attestazioni/generate";
}

export default function LoginPage() {
  const navigate = useNavigate();
  const location = useLocation();
  const { phase, error: sessionError, login, user } = useAuth();
  const [username, setUsername] = useState("operator");
  const [password, setPassword] = useState("");
  const [pin, setPin] = useState("");
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const returnTo = resolveReturnPath(location.state);

  if (phase === "authenticated" && user) {
    return <Navigate to={returnTo} replace />;
  }

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setIsSubmitting(true);
    setSubmitError(null);

    try {
      await login({
        username,
        password,
        pin,
      });
      setPassword("");
      setPin("");
      navigate(returnTo, { replace: true });
    } catch (caughtError) {
      setPassword("");
      setPin("");
      setSubmitError(caughtError instanceof Error ? caughtError.message : "Не вдалося увійти.");
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="screen-center">
      <div className="login-card">
        <div className="login-card__intro">
          <p className="eyebrow">UPPI Web UI</p>
          <h1>Вхід до UPPI Web Shell</h1>
          <p>
            Цей login працює тільки з web session (`/auth/login`, `/auth/me`, `/auth/logout`)
            і не є AE/SISTER аутентифікацією.
          </p>
        </div>

        <form className="form-grid" onSubmit={handleSubmit}>
          <label className="field">
            <span>Username</span>
            <input
              autoComplete="username"
              value={username}
              onChange={(event) => setUsername(event.target.value)}
            />
          </label>

          <label className="field">
            <span>Password</span>
            <input
              autoComplete="current-password"
              type="password"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
            />
          </label>

          <label className="field">
            <span>PIN</span>
            <input
              autoComplete="one-time-code"
              inputMode="numeric"
              type="password"
              value={pin}
              onChange={(event) => setPin(event.target.value)}
            />
          </label>

          <button className="primary-button" type="submit" disabled={isSubmitting || phase === "loading"}>
            {isSubmitting ? "Виконую вхід..." : "Увійти в shell"}
          </button>
        </form>

        {(sessionError || submitError) && (
          <div className="inline-alert" role="alert">
            {submitError || sessionError}
          </div>
        )}
      </div>
    </div>
  );
}
