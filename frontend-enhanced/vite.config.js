import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Point this at your FastAPI backend during development.
// In production, serve behind the same origin or set VITE_API_BASE_URL.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
});
