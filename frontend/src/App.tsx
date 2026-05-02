import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";

import { AuthProvider } from "./auth/AuthContext";
import AppLayout from "./components/AppLayout";
import AuthGuard from "./components/AuthGuard";
import BulkImportPage from "./pages/BulkImportPage";
import GenerateAttestazionePage from "./pages/GenerateAttestazionePage";
import JobStatusPage from "./pages/JobStatusPage";
import LoginPage from "./pages/LoginPage";

function AppRoutes() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route
        element={
          <AuthGuard>
            <AppLayout />
          </AuthGuard>
        }
      >
        <Route path="/" element={<Navigate to="/attestazioni/generate" replace />} />
        <Route path="/attestazioni/generate" element={<GenerateAttestazionePage />} />
        <Route path="/clients/bulk-import" element={<BulkImportPage />} />
        <Route path="/jobs" element={<JobStatusPage />} />
      </Route>
      <Route path="*" element={<Navigate to="/attestazioni/generate" replace />} />
    </Routes>
  );
}

export default function App() {
  return (
    <BrowserRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <AuthProvider>
        <AppRoutes />
      </AuthProvider>
    </BrowserRouter>
  );
}
