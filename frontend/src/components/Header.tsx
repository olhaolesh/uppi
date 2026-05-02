import { useState } from "react";
import { useNavigate } from "react-router-dom";

import { useAuth } from "../auth/AuthContext";

export default function Header() {
  const navigate = useNavigate();
  const { logout, user } = useAuth();
  const [isSubmitting, setIsSubmitting] = useState(false);

  const handleLogout = async () => {
    setIsSubmitting(true);
    try {
      await logout();
      navigate("/login", { replace: true });
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <header className="app-header">
      <div>
        <p className="eyebrow">UPPI Frontend Skeleton</p>
        <h1>Session-aware shell для майбутнього web UI</h1>
      </div>
      <div className="app-header__actions">
        <div className="user-pill">
          <span className="user-pill__label">Оператор</span>
          <strong>{user?.username || "unknown"}</strong>
        </div>
        <button
          className="ghost-button"
          type="button"
          onClick={handleLogout}
          disabled={isSubmitting}
        >
          {isSubmitting ? "Вихід..." : "Вийти із web session"}
        </button>
      </div>
    </header>
  );
}
