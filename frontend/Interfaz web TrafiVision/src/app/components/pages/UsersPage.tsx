import { useEffect, useState } from 'react';
import { Users, UserPlus, Shield, User, Trash2, Edit, AlertCircle } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Button } from '../ui/Button';
import { Badge } from '../ui/Badge';
import { Modal } from '../ui/Modal';

const API_URL = 'http://localhost:8000';

interface UsersPageProps {
  onShowToast: (message: string, type: 'success' | 'error' | 'info') => void;
}

interface AppUser {
  id: number;
  name: string;
  email: string;
  role: 'admin' | 'user';
  status: 'active' | 'inactive';
  lastAccess: string;
}

export function UsersPage({ onShowToast }: UsersPageProps) {
  // Aquí guardo los usuarios reales que vienen de MariaDB.
  const [users, setUsers] = useState<AppUser[]>([]);

  // Estados de carga y errores para mejorar la usabilidad.
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  // Estados del modal.
  const [showModal, setShowModal] = useState(false);
  const [modalAction, setModalAction] = useState<{ type: string; user?: AppUser }>({ type: '' });

  // Formulario usado para crear usuario o cambiar rol.
  const [formData, setFormData] = useState({
    name: '',
    email: '',
    password: '',
    role: 'user',
  });

  // Cargo los usuarios al entrar en la pantalla.
  useEffect(() => {
    cargarUsuarios();
  }, []);

  const cargarUsuarios = async () => {
    try {
      setLoading(true);
      setError('');

      const response = await fetch(`${API_URL}/api/admin/users`);

      if (!response.ok) {
        throw new Error('No se pudieron cargar los usuarios');
      }

      const data = await response.json();
      setUsers(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error cargando usuarios');
    } finally {
      setLoading(false);
    }
  };

  const handleAction = (type: string, user?: AppUser) => {
    setModalAction({ type, user });

    // Si voy a cambiar rol, relleno el select con el rol actual del usuario.
    if (user && type === 'role') {
      setFormData({
        name: '',
        email: '',
        password: '',
        role: user.role,
      });
    }

    // Si voy a crear usuario, limpio el formulario.
    if (type === 'add') {
      setFormData({
        name: '',
        email: '',
        password: '',
        role: 'user',
      });
    }

    setShowModal(true);
  };

  const handleConfirm = async () => {
    try {
      if (modalAction.type === 'add') {
        if (!formData.name || !formData.email || !formData.password) {
          onShowToast('Completa todos los campos', 'error');
          return;
        }

        // Desde esta pantalla el admin crea usuarios.
        // Por decisión del proyecto, el alta normal siempre será usuario.
        const response = await fetch(`${API_URL}/api/admin/users`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            nombre: formData.name,
            email: formData.email,
            password: formData.password,
          }),
        });

        const data = await response.json();

        if (!response.ok) {
          throw new Error(data.detail || 'Error creando usuario');
        }

        onShowToast('Usuario creado correctamente', 'success');
      }

      if (modalAction.type === 'role' && modalAction.user) {
        // Solo el admin puede convertir usuarios en admin desde esta pantalla.
        const response = await fetch(
          `${API_URL}/api/admin/users/${modalAction.user.id}/role?role=${formData.role}`,
          {
            method: 'PUT',
          }
        );

        const data = await response.json();

        if (!response.ok) {
          throw new Error(data.detail || 'Error actualizando rol');
        }

        onShowToast('Rol actualizado correctamente', 'success');
      }

      if (modalAction.type === 'delete' && modalAction.user) {
        const response = await fetch(`${API_URL}/api/admin/users/${modalAction.user.id}`, {
          method: 'DELETE',
        });

        const data = await response.json();

        if (!response.ok) {
          throw new Error(data.detail || 'Error eliminando usuario');
        }

        onShowToast('Usuario eliminado correctamente', 'success');
      }

      setShowModal(false);
      await cargarUsuarios();
    } catch (err) {
      onShowToast(err instanceof Error ? err.message : 'Error inesperado', 'error');
    }
  };

  const totalUsers = users.length;
  const activeUsers = users.filter((u) => u.status === 'active').length;
  const adminUsers = users.filter((u) => u.role === 'admin').length;
  const normalUsers = users.filter((u) => u.role === 'user').length;

  if (loading) {
    return <p className="text-muted-foreground">Cargando usuarios desde MariaDB...</p>;
  }

  if (error) {
    return (
      <Card>
        <CardContent className="p-6 text-destructive">{error}</CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl mb-2">Gestión de Usuarios</h2>
          <p className="text-muted-foreground">Administra usuarios y permisos del sistema</p>
        </div>

        <Button variant="primary" onClick={() => handleAction('add')}>
          <UserPlus className="h-4 w-4" />
          Nuevo Usuario
        </Button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center gap-4">
              <Users className="h-6 w-6 text-primary" />
              <div>
                <p className="text-2xl">{totalUsers}</p>
                <p className="text-sm text-muted-foreground">Total Usuarios</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center gap-4">
              <Users className="h-6 w-6 text-success" />
              <div>
                <p className="text-2xl">{activeUsers}</p>
                <p className="text-sm text-muted-foreground">Activos</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center gap-4">
              <Shield className="h-6 w-6 text-warning" />
              <div>
                <p className="text-2xl">{adminUsers}</p>
                <p className="text-sm text-muted-foreground">Administradores</p>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center gap-4">
              <User className="h-6 w-6 text-muted-foreground" />
              <div>
                <p className="text-2xl">{normalUsers}</p>
                <p className="text-sm text-muted-foreground">Usuarios Normales</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Lista de Usuarios</CardTitle>
        </CardHeader>

        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-3 px-4 text-sm">Nombre</th>
                  <th className="text-left py-3 px-4 text-sm">Email</th>
                  <th className="text-left py-3 px-4 text-sm">Rol</th>
                  <th className="text-left py-3 px-4 text-sm">Estado</th>
                  <th className="text-left py-3 px-4 text-sm">Creado</th>
                  <th className="text-left py-3 px-4 text-sm">Acciones</th>
                </tr>
              </thead>

              <tbody>
                {users.map((user) => (
                  <tr key={user.id} className="border-b border-border last:border-0 hover:bg-muted/50">
                    <td className="py-3 px-4">
                      <div className="flex items-center gap-3">
                        <div className="w-8 h-8 rounded-full bg-primary flex items-center justify-center text-white text-sm">
                          {user.name.charAt(0).toUpperCase()}
                        </div>
                        <span className="text-sm">{user.name}</span>
                      </div>
                    </td>

                    <td className="py-3 px-4 text-sm">{user.email}</td>

                    <td className="py-3 px-4">
                      <Badge variant={user.role === 'admin' ? 'warning' : 'default'}>
                        {user.role === 'admin' ? 'Administrador' : 'Usuario'}
                      </Badge>
                    </td>

                    <td className="py-3 px-4">
                      <Badge variant={user.status === 'active' ? 'success' : 'secondary'}>
                        {user.status === 'active' ? 'Activo' : 'Inactivo'}
                      </Badge>
                    </td>

                    <td className="py-3 px-4 text-sm">{user.lastAccess}</td>

                    <td className="py-3 px-4">
                      <div className="flex items-center gap-2">
                        <Button variant="ghost" size="sm" onClick={() => handleAction('role', user)}>
                          <Edit className="h-4 w-4" />
                        </Button>

                        <Button variant="ghost" size="sm" onClick={() => handleAction('delete', user)}>
                          <Trash2 className="h-4 w-4 text-destructive" />
                        </Button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      <Modal
        isOpen={showModal}
        onClose={() => setShowModal(false)}
        title={
          modalAction.type === 'add'
            ? 'Crear Nuevo Usuario'
            : modalAction.type === 'role'
              ? 'Cambiar Rol de Usuario'
              : 'Eliminar Usuario'
        }
        onConfirm={handleConfirm}
        confirmText={modalAction.type === 'delete' ? 'Eliminar Usuario' : 'Guardar Cambios'}
        confirmVariant={modalAction.type === 'delete' ? 'destructive' : 'primary'}
      >
        {modalAction.type === 'add' && (
          <div className="space-y-4">
            <div>
              <label className="block text-sm mb-2">Nombre Completo *</label>
              <input
                type="text"
                value={formData.name}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                className="w-full px-4 py-2 bg-input-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring"
              />
            </div>

            <div>
              <label className="block text-sm mb-2">Email *</label>
              <input
                type="email"
                value={formData.email}
                onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                className="w-full px-4 py-2 bg-input-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring"
              />
            </div>

            <div>
              <label className="block text-sm mb-2">Contraseña temporal *</label>
              <input
                type="password"
                value={formData.password}
                onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                className="w-full px-4 py-2 bg-input-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring"
              />
            </div>

            <div className="p-3 bg-primary/5 border border-primary/20 rounded-lg">
              <div className="flex items-start gap-2">
                <AlertCircle className="h-4 w-4 text-primary mt-0.5 flex-shrink-0" />
                <p className="text-xs text-muted-foreground">
                  Los nuevos usuarios se crean como usuario normal. Solo después un administrador puede cambiar su rol.
                </p>
              </div>
            </div>
          </div>
        )}

        {modalAction.type === 'role' && modalAction.user && (
          <div className="space-y-4">
            <p className="text-sm">Vas a cambiar el rol del usuario:</p>

            <div className="p-3 bg-muted rounded-lg">
              <p className="text-sm">
                <strong>{modalAction.user.name}</strong>
              </p>
              <p className="text-xs text-muted-foreground">{modalAction.user.email}</p>
            </div>

            <div>
              <label className="block text-sm mb-2">Nuevo Rol *</label>
              <select
                value={formData.role}
                onChange={(e) => setFormData({ ...formData, role: e.target.value })}
                className="w-full px-4 py-2 bg-input-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring"
              >
                <option value="user">Usuario</option>
                <option value="admin">Administrador</option>
              </select>
            </div>
          </div>
        )}

        {modalAction.type === 'delete' && modalAction.user && (
          <div className="space-y-4">
            <div className="p-4 bg-destructive/10 border border-destructive/20 rounded-lg">
              <div className="flex items-start gap-2">
                <AlertCircle className="h-5 w-5 text-destructive flex-shrink-0 mt-0.5" />
                <div>
                  <p className="text-sm">
                    ¿Seguro que quieres eliminar a <strong>{modalAction.user.name}</strong>?
                  </p>
                  <p className="text-xs text-muted-foreground mt-2">
                    Esta acción eliminará el usuario de la base de datos.
                  </p>
                </div>
              </div>
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
}