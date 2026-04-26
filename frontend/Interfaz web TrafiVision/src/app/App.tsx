import { useState } from 'react';
import { Sidebar } from './components/layout/Sidebar';
import { Header } from './components/layout/Header';
import { Toast } from './components/ui/Toast';
import { LandingPage } from './components/pages/LandingPage';
import { LoginPage } from './components/pages/LoginPage';
import { RegisterPage } from './components/pages/RegisterPage';
import { Dashboard } from './components/pages/Dashboard';
import { PredictionPage } from './components/pages/PredictionPage';
import { HistoryPage } from './components/pages/HistoryPage';
import { CamerasPage } from './components/pages/CamerasPage';
import { AdminPageSimplified } from './components/pages/AdminPageSimplified';
import { TrainingPageSimplified } from './components/pages/TrainingPageSimplified';
import { ScrapingPage } from './components/pages/ScrapingPage';
import { UsersPage } from './components/pages/UsersPage';
import { ProfilePage } from './components/pages/ProfilePage';

type UserRole = 'guest' | 'user' | 'admin';
type Page = 'landing' | 'login' | 'register' | 'dashboard' | 'prediction' | 'history' | 'cameras' | 'admin' | 'training' | 'scraping' | 'users' | 'profile';

interface ToastMessage {
  id: number;
  message: string;
  type: 'success' | 'error' | 'info';
}

export default function App() {
  const [userRole, setUserRole] = useState<UserRole>('guest');
  const [currentPage, setCurrentPage] = useState<Page>('landing');
  const [toasts, setToasts] = useState<ToastMessage[]>([]);
  const [toastId, setToastId] = useState(0);

  const showToast = (message: string, type: 'success' | 'error' | 'info') => {
    const id = toastId + 1;
    setToastId(id);
    setToasts(prev => [...prev, { id, message, type }]);
  };

  const removeToast = (id: number) => {
    setToasts(prev => prev.filter(toast => toast.id !== id));
  };

  const handleLogin = (role: 'user' | 'admin') => {
    setUserRole(role);
    setCurrentPage('dashboard');
    showToast(`Bienvenido a TrafiVision`, 'success');
  };

  const handleRegisterSuccess = () => {
    showToast('Cuenta creada correctamente. Inicia sesión para continuar', 'success');
    setCurrentPage('login');
  };

  const handleLogout = () => {
    setUserRole('guest');
    setCurrentPage('landing');
    showToast('Sesión cerrada correctamente', 'info');
  };

  const handleNavigate = (page: string) => {
    setCurrentPage(page as Page);
  };

  const getPageTitle = () => {
    const titles: Record<Page, { title: string; subtitle?: string }> = {
      landing: { title: 'TrafiVision' },
      login: { title: 'Iniciar Sesión' },
      register: { title: 'Crear Cuenta' },
      dashboard: { title: 'Dashboard', subtitle: 'Resumen del estado del tráfico en Madrid' },
      prediction: { title: 'Predicción de Tráfico', subtitle: 'Calcula predicciones personalizadas' },
      history: { title: 'Histórico de Tráfico', subtitle: 'Analiza datos y tendencias históricas' },
      cameras: { title: 'Cámaras de Tráfico', subtitle: 'Monitoreo en tiempo real' },
      admin: { title: 'Administración', subtitle: 'Gestión del sistema' },
      training: { title: 'Entrenamiento ML', subtitle: 'Gestión de modelos de Machine Learning' },
      scraping: { title: 'Actualización de Datos', subtitle: 'Scraping y procesamiento de datos' },
      users: { title: 'Gestión de Usuarios', subtitle: 'Administra usuarios y permisos' },
      profile: { title: 'Mi Perfil', subtitle: 'Configuración de tu cuenta' },
    };
    return titles[currentPage];
  };

  const renderPage = () => {
    switch (currentPage) {
      case 'landing':
        return (
          <LandingPage
            onLogin={() => setCurrentPage('login')}
            onDemo={() => {
              setUserRole('user');
              setCurrentPage('dashboard');
              showToast('Accediendo en modo demostración', 'info');
            }}
          />
        );
      case 'login':
        return (
          <LoginPage
            onLogin={handleLogin}
            onBack={() => setCurrentPage('landing')}
            onGoToRegister={() => setCurrentPage('register')}
          />
        );
      case 'register':
        return (
          <RegisterPage
            onRegisterSuccess={handleRegisterSuccess}
            onGoToLogin={() => setCurrentPage('login')}
          />
        );
      case 'dashboard':
        return <Dashboard />;
      case 'prediction':
        return <PredictionPage />;
      case 'history':
        return <HistoryPage />;
      case 'cameras':
        return <CamerasPage />;
      case 'admin':
        return <AdminPageSimplified onShowToast={showToast} />;
      case 'training':
        return <TrainingPageSimplified onShowToast={showToast} />;
      case 'scraping':
        return <ScrapingPage />;
      case 'users':
        return <UsersPage onShowToast={showToast} />;
      case 'profile':
        return <ProfilePage userRole={userRole} onLogout={handleLogout} />;
      default:
        return <Dashboard />;
    }
  };

  if (userRole === 'guest') {
    return (
      <>
        {renderPage()}
        {toasts.map(toast => (
          <Toast
            key={toast.id}
            message={toast.message}
            type={toast.type}
            onClose={() => removeToast(toast.id)}
          />
        ))}
      </>
    );
  }

  return (
    <div className="flex h-screen bg-background">
      <Sidebar
        currentPage={currentPage}
        onNavigate={handleNavigate}
        userRole={userRole}
      />

      <div className="flex-1 flex flex-col overflow-hidden">
        <Header
          title={getPageTitle().title}
          subtitle={getPageTitle().subtitle}
        />

        <main className="flex-1 overflow-y-auto p-6">
          {renderPage()}
        </main>
      </div>

      {toasts.map(toast => (
        <Toast
          key={toast.id}
          message={toast.message}
          type={toast.type}
          onClose={() => removeToast(toast.id)}
        />
      ))}
    </div>
  );
}
