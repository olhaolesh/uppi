import { Outlet } from "react-router-dom";

import Header from "./Header";
import Navigation from "./Navigation";

export default function AppLayout() {
  return (
    <div className="app-shell">
      <Header />
      <div className="app-shell__body">
        <Navigation />
        <main className="app-content">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
