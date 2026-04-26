import { Camera, TrendingUp, History, Cloud, Brain, BarChart3 } from 'lucide-react';
import { Button } from '../ui/Button';

interface LandingPageProps {
  onLogin: () => void;
  onDemo: () => void;
}

export function LandingPage({ onLogin, onDemo }: LandingPageProps) {
  const features = [
    {
      icon: TrendingUp,
      title: 'Predicción de Tráfico',
      description: 'Modelos de ML para predecir el nivel de tráfico en tiempo real',
    },
    {
      icon: History,
      title: 'Análisis Histórico',
      description: 'Visualiza patrones y tendencias del tráfico urbano',
    },
    {
      icon: Camera,
      title: 'Cámaras de Tráfico',
      description: 'Monitorización continua de múltiples puntos de la ciudad',
    },
    {
      icon: Cloud,
      title: 'Datos Meteorológicos',
      description: 'Integración de condiciones climáticas en las predicciones',
    },
    {
      icon: Brain,
      title: 'Machine Learning',
      description: 'Algoritmos avanzados para mayor precisión',
    },
    {
      icon: BarChart3,
      title: 'Análisis Visual',
      description: 'Gráficas y dashboards interactivos',
    },
  ];

  return (
    <div className="min-h-screen bg-gradient-to-b from-[#0f172a] to-[#1e293b]">
      <nav className="border-b border-white/10">
        <div className="max-w-7xl mx-auto px-6 py-4 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-lg bg-primary flex items-center justify-center">
              <Camera className="h-6 w-6 text-white" />
            </div>
            <h1 className="text-xl text-white">TrafiVision</h1>
          </div>
          <Button variant="primary" onClick={onLogin}>
            Iniciar Sesión
          </Button>
        </div>
      </nav>

      <section className="max-w-7xl mx-auto px-6 py-20 text-center">
        <h2 className="text-5xl text-white mb-6">
          Predicción Inteligente del Tráfico Urbano en Madrid
        </h2>
        <p className="text-xl text-gray-300 mb-8 max-w-3xl mx-auto">
          Utiliza datos en tiempo real, análisis histórico y Machine Learning para
          optimizar tus desplazamientos por la ciudad
        </p>
        <div className="flex items-center justify-center gap-4">
          <Button variant="primary" size="lg" onClick={onLogin}>
            Iniciar Sesión
          </Button>
          <Button variant="ghost" size="lg" onClick={onDemo} className="text-white border border-white/20 hover:bg-white/10">
            Ver Demo
          </Button>
        </div>
      </section>

      <section className="max-w-7xl mx-auto px-6 py-16">
        <h3 className="text-3xl text-white text-center mb-12">Funcionalidades</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {features.map((feature) => {
            const Icon = feature.icon;
            return (
              <div
                key={feature.title}
                className="bg-white/5 backdrop-blur border border-white/10 rounded-lg p-6 hover:bg-white/10 transition-colors"
              >
                <div className="w-12 h-12 rounded-lg bg-primary/20 flex items-center justify-center mb-4">
                  <Icon className="h-6 w-6 text-primary" />
                </div>
                <h4 className="text-lg text-white mb-2">{feature.title}</h4>
                <p className="text-gray-400 text-sm">{feature.description}</p>
              </div>
            );
          })}
        </div>
      </section>

      <section className="max-w-7xl mx-auto px-6 py-16">
        <div className="bg-white/5 backdrop-blur border border-white/10 rounded-lg p-8">
          <div className="aspect-video bg-[#1e293b] rounded-lg flex items-center justify-center">
            <div className="text-center">
              <BarChart3 className="h-16 w-16 text-primary mx-auto mb-4" />
              <p className="text-gray-400">Vista previa del Dashboard</p>
            </div>
          </div>
        </div>
      </section>

      <footer className="border-t border-white/10 mt-20">
        <div className="max-w-7xl mx-auto px-6 py-8 text-center text-gray-400 text-sm">
          <p>© 2026 TrafiVision. Sistema de Predicción de Tráfico Urbano.</p>
        </div>
      </footer>
    </div>
  );
}
