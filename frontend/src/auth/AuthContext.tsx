import {
  createContext,
  startTransition,
  useContext,
  useEffect,
  useEffectEvent,
  useState,
  type ReactNode,
} from "react";

import { getCurrentSession, loginRequest, logoutRequest } from "../api/auth";
import { ApiError } from "../api/client";
import type { AuthStatusResponse, AuthenticatedUser, LoginRequest } from "../types/api";

type AuthPhase = "loading" | "authenticated" | "anonymous";

type AuthContextValue = {
  phase: AuthPhase;
  user: AuthenticatedUser | null;
  error: string | null;
  login: (payload: LoginRequest) => Promise<void>;
  logout: () => Promise<void>;
  refresh: () => Promise<void>;
};

const AuthContext = createContext<AuthContextValue | undefined>(undefined);

function resolveSessionUser(response: AuthStatusResponse): AuthenticatedUser | null {
  if (!response.authenticated || !response.user) {
    return null;
  }
  return response.user;
}

function isUnauthorizedError(error: unknown): boolean {
  if (error instanceof ApiError) {
    return error.status === 401;
  }
  if (typeof error !== "object" || error === null || !("status" in error)) {
    return false;
  }
  return (error as { status?: unknown }).status === 401;
}

export function AuthProvider({ children }: { children: ReactNode }) {
  const [phase, setPhase] = useState<AuthPhase>("loading");
  const [user, setUser] = useState<AuthenticatedUser | null>(null);
  const [error, setError] = useState<string | null>(null);

  const applySessionState = (nextUser: AuthenticatedUser | null, nextError: string | null = null) => {
    startTransition(() => {
      setUser(nextUser);
      setPhase(nextUser ? "authenticated" : "anonymous");
      setError(nextError);
    });
  };

  const refresh = useEffectEvent(async () => {
    try {
      const response = await getCurrentSession();
      applySessionState(resolveSessionUser(response));
    } catch (caughtError) {
      if (isUnauthorizedError(caughtError)) {
        applySessionState(null);
        return;
      }

      applySessionState(null, "Не вдалося перевірити веб-сесію. Перевір backend shell.");
    }
  });

  useEffect(() => {
    void refresh();
  }, []);

  const login = async (payload: LoginRequest) => {
    startTransition(() => {
      setError(null);
    });

    const response = await loginRequest(payload);
    applySessionState(resolveSessionUser(response));
  };

  const logout = async () => {
    try {
      await logoutRequest();
    } finally {
      applySessionState(null);
    }
  };

  return (
    <AuthContext.Provider value={{ phase, user, error, login, logout, refresh }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within AuthProvider.");
  }
  return context;
}
