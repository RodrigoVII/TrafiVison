import { useEffect, useState } from 'react';
import { User, Mail, Lock, LogOut, AlertCircle } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Button } from '../ui/Button';
import { Badge } from '../ui/Badge';
import { Alert } from '../ui/Alert';

interface ProfilePageProps {
  userRole: 'user' | 'admin';
  onLogout: () => void;
}

interface StoredUser {
  nombre: string;
  email: string;
  role: 'user' | 'admin';
  token?: string;
}

export function ProfilePage({ userRole, onLogout }: ProfilePageProps) {
  const [user, setUser] = useState<StoredUser | null>(null);

  const [currentPassword, setCurrentPassword] = useState('');
  const [newPassword, setNewPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');

  const [successMessage, setSuccessMessage] = useState('');
  const [error, setError] = useState('');

  // Cargo el usuario real que guardamos al hacer login.
  useEffect(() => {
    const storedUser = localStorage.getItem('trafivision_user');

    if (storedUser) {
      setUser(JSON.parse(storedUser));
    }
  }, []);

  const role = user?.role ?? userRole;
  const name = user?.nombre ?? 'Usuario Demo';
  const email = user?.email ?? (role === 'admin' ? 'admin@trafivision.com' : 'usuario@demo.com');

  const handleChangePassword = (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setSuccessMessage('');

    // Para esta fase dejo el cambio de contraseña como validación local.
    // La tabla usuario ya guarda contraseña, pero este endpoint se podría añadir después.
    if (!currentPassword || !newPassword || !confirmPassword) {
      setError('Completa todos los campos');
      return;
    }

    if (newPassword.length < 6) {
      setError('La nueva contraseña debe tener al menos 6 caracteres');
      return;
    }

    if (newPassword !== confirmPassword) {
      setError('Las contraseñas no coinciden');
      return;
    }

    setSuccessMessage('Contraseña validada correctamente para la demo');
    setCurrentPassword('');
    setNewPassword('');
    setConfirmPassword('');
  };

  const handleLogout = () => {
    localStorage.removeItem('trafivision_user');
    onLogout();
  };

  return (
    <div className="max-w-3xl space-y-6">
      <div>
        <h2 className="text-2xl mb-2">Perfil de Usuario</h2>
        <p className="text-muted-foreground">Consulta tu información de sesión y preferencias</p>
      </div>

      {successMessage && (
        <Alert variant="success" onClose={() => setSuccessMessage('')}>
          {successMessage}
        </Alert>
      )}

      {error && (
        <Alert variant="error" onClose={() => setError('')}>
          <div className="flex items-center gap-2">
            <AlertCircle className="h-4 w-4" />
            {error}
          </div>
        </Alert>
      )}

      <Card>
        <CardHeader>
          <CardTitle>Información Personal</CardTitle>
        </CardHeader>

        <CardContent className="space-y-6">
          <div className="flex items-center gap-4">
            <div className="w-20 h-20 rounded-full bg-primary flex items-center justify-center text-white text-2xl">
              {name.charAt(0).toUpperCase()}
            </div>

            <div>
              <h3 className="text-lg">{name}</h3>
              <p className="text-sm text-muted-foreground">{email}</p>

              <Badge variant={role === 'admin' ? 'warning' : 'default'} className="mt-2">
                {role === 'admin' ? 'Administrador' : 'Usuario'}
              </Badge>
            </div>
          </div>

          <div className="space-y-4">
            <div>
              <label className="flex items-center gap-2 text-sm mb-2">
                <User className="h-4 w-4" />
                Nombre Completo
              </label>

              <input
                type="text"
                value={name}
                disabled
                className="w-full px-4 py-2 bg-input-background border border-input rounded-lg opacity-80"
              />
            </div>

            <div>
              <label className="flex items-center gap-2 text-sm mb-2">
                <Mail className="h-4 w-4" />
                Email
              </label>

              <input
                type="email"
                value={email}
                disabled
                className="w-full px-4 py-2 bg-input-background border border-input rounded-lg opacity-80"
              />

              <p className="text-xs text-muted-foreground mt-1">
                El email se obtiene del login y no se modifica desde esta pantalla.
              </p>
            </div>

            <div>
              <label className="block text-sm mb-2">Rol</label>

              <Badge variant={role === 'admin' ? 'warning' : 'default'}>
                {role === 'admin' ? 'Administrador' : 'Usuario'}
              </Badge>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Lock className="h-5 w-5" />
            Cambiar Contraseña
          </CardTitle>
        </CardHeader>

        <CardContent>
          <form onSubmit={handleChangePassword} className="space-y-4">
            <div>
              <label className="block text-sm mb-2">Contraseña Actual</label>

              <input
                type="password"
                value={currentPassword}
                onChange={(e) => setCurrentPassword(e.target.value)}
                className="w-full px-4 py-2 bg-input-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder="••••••••"
              />
            </div>

            <div>
              <label className="block text-sm mb-2">Nueva Contraseña</label>

              <input
                type="password"
                value={newPassword}
                onChange={(e) => setNewPassword(e.target.value)}
                className="w-full px-4 py-2 bg-input-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder="Mínimo 6 caracteres"
              />
            </div>

            <div>
              <label className="block text-sm mb-2">Confirmar Nueva Contraseña</label>

              <input
                type="password"
                value={confirmPassword}
                onChange={(e) => setConfirmPassword(e.target.value)}
                className="w-full px-4 py-2 bg-input-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring"
                placeholder="Repite la contraseña"
              />
            </div>

            <Button type="submit" variant="primary">
              Validar Cambio
            </Button>
          </form>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Preferencias</CardTitle>
        </CardHeader>

        <CardContent className="space-y-4">
          <div className="flex items-center justify-between p-4 bg-muted rounded-lg">
            <div>
              <p className="text-sm">Notificaciones por Email</p>
              <p className="text-xs text-muted-foreground">Recibe alertas importantes del sistema</p>
            </div>

            <input type="checkbox" defaultChecked />
          </div>

          <div className="flex items-center justify-between p-4 bg-muted rounded-lg">
            <div>
              <p className="text-sm">Alertas de Cámaras</p>
              <p className="text-xs text-muted-foreground">Notificar cuando una cámara falle</p>
            </div>

            <input type="checkbox" defaultChecked />
          </div>
        </CardContent>
      </Card>

      <Card className="border-destructive/20">
        <CardHeader>
          <CardTitle className="text-destructive">Cerrar Sesión</CardTitle>
        </CardHeader>

        <CardContent>
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm">Salir de tu cuenta</p>
              <p className="text-xs text-muted-foreground">Se eliminará la sesión guardada en el navegador</p>
            </div>

            <Button variant="destructive" onClick={handleLogout}>
              <LogOut className="h-4 w-4" />
              Cerrar Sesión
            </Button>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}