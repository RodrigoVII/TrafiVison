import { useState } from 'react';
import { Camera, Mail, Lock, User, AlertCircle } from 'lucide-react';
import { Button } from '../ui/Button';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Alert } from '../ui/Alert';

const API_URL = 'http://localhost:8000';

interface RegisterPageProps {
  onRegisterSuccess: () => void;
  onGoToLogin: () => void;
}

export function RegisterPage({ onRegisterSuccess, onGoToLogin }: RegisterPageProps) {

  // ============================
  // Estado del formulario
  // ============================

  const [formData, setFormData] = useState({
    name: '',
    email: '',
    password: '',
    confirmPassword: '',
  });

  const [errors, setErrors] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);

  // ============================
  // Validación
  // ============================

  const validateForm = () => {
    const newErrors: string[] = [];

    if (!formData.name.trim()) {
      newErrors.push('El nombre es obligatorio');
    }

    if (!formData.email.trim()) {
      newErrors.push('El email es obligatorio');
    } else if (!/\S+@\S+\.\S+/.test(formData.email)) {
      newErrors.push('El email no es válido');
    }

    if (!formData.password) {
      newErrors.push('La contraseña es obligatoria');
    } else if (formData.password.length < 6) {
      newErrors.push('La contraseña debe tener al menos 6 caracteres');
    }

    if (formData.password !== formData.confirmPassword) {
      newErrors.push('Las contraseñas no coinciden');
    }

    setErrors(newErrors);
    return newErrors.length === 0;
  };

  // ============================
  // SUBMIT REAL (API)
  // ============================

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!validateForm()) return;

    setLoading(true);
    setErrors([]);

    try {
      // 👇 llamada REAL al backend
      const response = await fetch(`${API_URL}/api/auth/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          nombre: formData.name,
          email: formData.email,
          password: formData.password,
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || 'Error al registrar usuario');
      }

      // 👇 éxito → redirigimos al login
      onRegisterSuccess();

    } catch (err) {
      setErrors([
        err instanceof Error ? err.message : 'Error inesperado',
      ]);
    } finally {
      setLoading(false);
    }
  };

  // ============================
  // UI
  // ============================

  return (
    <div className="min-h-screen bg-gradient-to-b from-[#0f172a] to-[#1e293b] flex items-center justify-center p-6">
      <Card className="w-full max-w-md">
        <CardHeader className="text-center">
          <div className="w-16 h-16 rounded-lg bg-primary flex items-center justify-center mx-auto mb-4">
            <Camera className="h-8 w-8 text-white" />
          </div>

          <CardTitle className="text-2xl">Crear Cuenta</CardTitle>

          <p className="text-sm text-muted-foreground mt-2">
            Solo puedes registrarte como usuario estándar
          </p>
        </CardHeader>

        <CardContent>
          <form onSubmit={handleSubmit} className="space-y-4">

            {/* ERRORES */}
            {errors.length > 0 && (
              <Alert variant="error">
                {errors.map((error, idx) => (
                  <div key={idx} className="flex items-center gap-2">
                    <AlertCircle className="h-4 w-4" />
                    <span className="text-sm">{error}</span>
                  </div>
                ))}
              </Alert>
            )}

            {/* NOMBRE */}
            <div>
              <label className="block text-sm mb-2">Nombre Completo</label>
              <div className="relative">
                <User className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
                <input
                  type="text"
                  value={formData.name}
                  onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                  className="w-full pl-10 pr-4 py-2 border rounded-lg"
                  disabled={loading}
                />
              </div>
            </div>

            {/* EMAIL */}
            <div>
              <label className="block text-sm mb-2">Email</label>
              <div className="relative">
                <Mail className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
                <input
                  type="email"
                  value={formData.email}
                  onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                  className="w-full pl-10 pr-4 py-2 border rounded-lg"
                  disabled={loading}
                />
              </div>
            </div>

            {/* PASSWORD */}
            <div>
              <label className="block text-sm mb-2">Contraseña</label>
              <div className="relative">
                <Lock className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
                <input
                  type="password"
                  value={formData.password}
                  onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                  className="w-full pl-10 pr-4 py-2 border rounded-lg"
                  disabled={loading}
                />
              </div>
            </div>

            {/* CONFIRM PASSWORD */}
            <div>
              <label className="block text-sm mb-2">Confirmar Contraseña</label>
              <div className="relative">
                <Lock className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
                <input
                  type="password"
                  value={formData.confirmPassword}
                  onChange={(e) => setFormData({ ...formData, confirmPassword: e.target.value })}
                  className="w-full pl-10 pr-4 py-2 border rounded-lg"
                  disabled={loading}
                />
              </div>
            </div>

            <Button type="submit" className="w-full" loading={loading}>
              Crear Cuenta
            </Button>

            <div className="text-center">
              <button onClick={onGoToLogin} className="text-sm text-primary">
                ¿Ya tienes cuenta? Inicia sesión
              </button>
            </div>

          </form>
        </CardContent>
      </Card>
    </div>
  );
}