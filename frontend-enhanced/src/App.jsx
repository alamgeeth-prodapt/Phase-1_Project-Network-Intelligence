import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { AuthProvider } from './context/AuthContext';
import { ThemeProvider } from './context/ThemeContext';
import ProtectedRoute from './components/ProtectedRoute';
import TopNav from './components/TopNav';
import AiNocAssistant from './components/AiNocAssistant';
import Login from './pages/Login';
import Dashboard from './pages/Dashboard';
import GridMap from './pages/GridMap';
import Alerts from './pages/Alerts';

function AppLayout({ children, padded = true }) {
  return (
    <div>
      <TopNav />
      <main style={padded ? { padding: '24px 28px' } : undefined}>{children}</main>
      <AiNocAssistant />
    </div>
  );
}

export default function App() {
  return (
    <ThemeProvider>
      <AuthProvider>
        <BrowserRouter>
          <Routes>
            <Route path="/login" element={<Login />} />
            <Route
              path="/"
              element={
                <ProtectedRoute>
                  <AppLayout>
                    <Dashboard />
                  </AppLayout>
                </ProtectedRoute>
              }
            />
            <Route
              path="/map"
              element={
                <ProtectedRoute>
                  <AppLayout padded={false}>
                    <GridMap />
                  </AppLayout>
                </ProtectedRoute>
              }
            />
            <Route
              path="/alerts"
              element={
                <ProtectedRoute>
                  <AppLayout>
                    <Alerts />
                  </AppLayout>
                </ProtectedRoute>
              }
            />
          </Routes>
        </BrowserRouter>
      </AuthProvider>
    </ThemeProvider>
  );
}
