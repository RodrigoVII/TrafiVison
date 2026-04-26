import { useState, type FormEvent } from 'react';
import { Camera, Mail, Lock, AlertCircle } from 'lucide-react';
import { Button } from '../ui/Button';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Alert } from '../ui/Alert';
import { loginRequest } from '../../services/api';

interface LoginPageProps {
  onLogin: (role: 'user' | 'admin') => void;
  onBack: () => void;
  onGoToRegister: () => void;
}

export function LoginPage({ onLogin, onBack, onGoToRegister }: LoginPageProps) {
  // Estados del formulario
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');

  // Estados de usabilidad: error y carga
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setError('');

    // Validación básica antes de llamar al backend
    if (!email || !password) {
      setError('Por favor, completa todos los campos');
      return;
    }

    if (!/\S+@\S+\.\S+/.test(email)) {
      setError('Por favor, introduce un email válido');
      return;
    }

    setLoading(true);

    try {
      // Login real contra FastAPI
      const user = await loginRequest(email, password);

      // Guardamos sesión básica para recordar el usuario mientras usa la app
      localStorage.setItem('trafivision_user', JSON.stringify(user));

      // Avisamos a App.tsx del rol para cargar menú de usuario/admin
      onLogin(user.role);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error al iniciar sesión');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gradient-to-b from-[#0f172a] to-[#1e293b] flex items-center justify-center p-6">
      <Card className="w-full max-w-md">
        <CardHeader className="text-center">
          <div className="w-16 h-16 rounded-lg bg-primary flex items-center justify-center mx-auto mb-4">
            <Camera className="h-8 w-8 text-white" />
          </div>

          <CardTitle className="text-2xl">Iniciar Sesión</CardTitle>

          <p className="text-sm text-muted-foreground mt-2">
            Accede a TrafiVision para gestionar y predecir el tráfico urbano
          </p>
        </CardHeader>

        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">
            {error && (
              <Alert variant="error">
                <div className="flex items-center gap-2">
                  <AlertCircle className="h-4 w-4" />
                  <span>{error}</span>
                </div>
              </Alert>
            )}

            <div>
              <label className="block text-sm mb-2">Email</label>
              <div className="relative">
                <Mail className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
                <input
                  type="email"
                  value={email}
                  disabled={loading}
                  onChange={(e) => setEmail(e.target.value)}
                  className="w-full pl-10 pr-4 py-2 bg-input-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring disabled:opacity-60"
                  placeholder="tu@email.com"
                />
              </div>
            </div>

            <div>
              <label className="block text-sm mb-2">Contraseña</label>
              <div className="relative">
                <Lock className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
                <input
                  type="password"
                  value={password}
                  disabled={loading}
                  onChange={(e) => setPassword(e.target.value)}
                  className="w-full pl-10 pr-4 py-2 bg-input-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring disabled:opacity-60"
                  placeholder="••••••••"
                />
              </div>
            </div>

            <Button type="submit" variant="primary" className="w-full" loading={loading}>
              {loading ? 'Iniciando sesión...' : 'Iniciar Sesión'}
            </Button>

            <div className="pt-4 border-t border-border space-y-3">
              <p className="text-xs text-muted-foreground text-center">
                Credenciales de demostración:
              </p>

              <div className="p-3 bg-muted rounded-lg space-y-2 text-xs">
                <div>
                  <p className="text-muted-foreground">Usuario:</p>
                  <p className="text-foreground">usuario@demo.com / demo123</p>
                </div>

                <div>
                  <p className="text-muted-foreground">Administrador:</p>
                  <p className="text-foreground">admin@trafivision.com / admin123</p>
                </div>
              </div>
            </div>

            <div className="flex gap-2">
              <Button type="button" variant="ghost" className="flex-1" onClick={onBack} disabled={loading}>
                Volver
              </Button>

              <Button type="button" variant="secondary" className="flex-1" onClick={onGoToRegister} disabled={loading}>
                Registrarse
              </Button>
            </div>
          </form>
        </CardContent>
      </Card>
    </div>
  );
}