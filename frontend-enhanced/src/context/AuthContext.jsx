import { createContext, useContext, useEffect, useState, useCallback } from 'react';
import { login as loginRequest } from '../api/client';

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [token, setToken] = useState(() => localStorage.getItem('gnc_token'));

  const login = useCallback(async (username, password) => {
    const data = await loginRequest(username, password);
    localStorage.setItem('gnc_token', data.access_token);
    setToken(data.access_token);
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem('gnc_token');
    setToken(null);
  }, []);

  // If any API call comes back 401, drop the session so the UI can redirect.
  useEffect(() => {
    const handleExpired = () => setToken(null);
    window.addEventListener('gnc-auth-expired', handleExpired);
    return () => window.removeEventListener('gnc-auth-expired', handleExpired);
  }, []);

  return (
    <AuthContext.Provider value={{ token, isAuthenticated: !!token, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error('useAuth must be used within AuthProvider');
  return ctx;
}
