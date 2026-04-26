import { useEffect, useState } from 'react';
import {
  LayoutDashboard,
  TrendingUp,
  History,
  Camera,
  Shield,
  Brain,
  Database,
  Users,
  User
} from 'lucide-react';
import { Badge } from '../ui/Badge';

interface SidebarProps {
  currentPage: string;
  onNavigate: (page: string) => void;
  userRole: 'guest' | 'user' | 'admin';
}

interface StoredUser {
  nombre: string;
  email: string;
  role: 'user' | 'admin';
}

export function Sidebar({ currentPage, onNavigate, userRole }: SidebarProps) {
  const [user, setUser] = useState<StoredUser | null>(null);

  useEffect(() => {
    const storedUser = localStorage.getItem('trafivision_user');
    if (storedUser) {
      setUser(JSON.parse(storedUser));
    }
  }, []);

  const realRole = user?.role ?? userRole;
  const userName = user?.nombre ?? 'Usuario';
  const userInitial = userName.charAt(0).toUpperCase();

  const userMenuItems = [
    { id: 'dashboard', label: 'Dashboard', icon: LayoutDashboard },
    { id: 'prediction', label: 'Predicción', icon: TrendingUp },
    { id: 'history', label: 'Histórico', icon: History },
    { id: 'cameras', label: 'Cámaras', icon: Camera },
  ];

  const adminMenuItems = [
    { id: 'admin', label: 'Administración', icon: Shield },
    { id: 'training', label: 'Entrenamiento ML', icon: Brain },
    { id: 'scraping', label: 'Actualización Datos', icon: Database },
    { id: 'users', label: 'Usuarios', icon: Users },
  ];

  const profileItem = {
    id: 'profile',
    label: 'Mi Perfil',
    icon: User,
  };

  const menuItems =
    realRole === 'admin'
      ? [...userMenuItems, profileItem, ...adminMenuItems]
      : [...userMenuItems, profileItem];

  return (
    <div className="w-64 h-screen bg-sidebar border-r border-sidebar-border flex flex-col">
      <div className="p-6 border-b border-sidebar-border">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 rounded-lg bg-primary flex items-center justify-center">
            <Camera className="h-6 w-6 text-white" />
          </div>

          <div>
            <h1 className="text-xl text-sidebar-foreground">TrafiVision</h1>
            <p className="text-xs text-sidebar-foreground/60">Análisis de Tráfico</p>
          </div>
        </div>
      </div>

      <nav className="flex-1 p-4 overflow-y-auto">
        <div className="flex flex-col gap-1">
          {menuItems.map((item) => {
            const Icon = item.icon;
            const isActive = currentPage === item.id;

            return (
              <button
                key={item.id}
                onClick={() => onNavigate(item.id)}
                className={`w-full flex items-center gap-3 px-3 py-2 rounded-lg transition-colors text-left ${
                  isActive
                    ? 'bg-sidebar-accent text-sidebar-accent-foreground'
                    : 'text-sidebar-foreground/70 hover:bg-sidebar-accent/50 hover:text-sidebar-foreground'
                }`}
              >
                <Icon className="h-5 w-5 flex-shrink-0" />
                <span className="text-sm block flex-1">{item.label}</span>
              </button>
            );
          })}
        </div>
      </nav>

      <div className="p-4 border-t border-sidebar-border">
        <div className="flex items-center gap-3 px-3 py-2">
          <div className="w-8 h-8 rounded-full bg-primary flex items-center justify-center text-white text-sm">
            {userInitial}
          </div>

          <div className="flex-1 min-w-0">
            <p className="text-sm text-sidebar-foreground truncate">{userName}</p>

            <Badge variant={realRole === 'admin' ? 'warning' : 'default'} className="mt-1">
              {realRole === 'admin' ? 'Administrador' : 'Usuario'}
            </Badge>
          </div>
        </div>
      </div>
    </div>
  );
}