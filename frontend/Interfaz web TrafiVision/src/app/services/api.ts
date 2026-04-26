const API_URL = 'http://localhost:8000';

export type UserRole = 'user' | 'admin';

export interface LoginResponse {
  nombre: string;
  email: string;
  role: UserRole;
  token: string;
}

export async function loginRequest(email: string, password: string): Promise<LoginResponse> {
  const response = await fetch(`${API_URL}/api/auth/login`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ email, password }),
  });

  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail || 'Error al iniciar sesión');
  }

  return response.json();
}
export async function getHistorico() {
  const res = await fetch(`${API_URL}/api/historico`);
  return res.json();
}