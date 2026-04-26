import { useState } from 'react';
import {
  Database,
  Brain,
  Globe,
  CheckCircle2,
  Clock,
  Play,
  AlertCircle,
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Button } from '../ui/Button';
import { Badge } from '../ui/Badge';
import { Modal } from '../ui/Modal';

const API_URL = 'http://localhost:8000';

interface AdminPageSimplifiedProps {
  onShowToast: (message: string, type: 'success' | 'error' | 'info') => void;
}

interface LogItem {
  time: string;
  type: 'success' | 'error' | 'info';
  message: string;
}

export function AdminPageSimplified({ onShowToast }: AdminPageSimplifiedProps) {
  const [showModal, setShowModal] = useState(false);
  const [modalAction, setModalAction] = useState('');
  const [loading, setLoading] = useState(false);

  const [lastScraping, setLastScraping] = useState('Pendiente');
  const [lastWeather, setLastWeather] = useState('Pendiente');
  const [lastTraining, setLastTraining] = useState('Pendiente');
  const [modelName, setModelName] = useState('Random Forest');
  const [accuracy, setAccuracy] = useState('Pendiente');

  const [logs, setLogs] = useState<LogItem[]>([
    {
      time: 'Sistema',
      type: 'info',
      message: 'Panel preparado. Las acciones se ejecutarán contra FastAPI.',
    },
  ]);

  const addLog = (type: 'success' | 'error' | 'info', message: string) => {
    setLogs((prev) => [
      {
        time: new Date().toLocaleTimeString(),
        type,
        message,
      },
      ...prev,
    ]);
  };

  const handleAction = (action: string) => {
    setModalAction(action);
    setShowModal(true);
  };

  const getEndpoint = () => {
    if (modalAction === 'Web Scraping') {
      return '/api/admin/scraping';
    }

    if (modalAction === 'Actualización de Clima') {
      return '/api/admin/clima';
    }

    if (modalAction === 'Entrenamiento de Modelo') {
      return '/api/admin/train?modelo=random_forest';
    }

    return '';
  };

  const confirmAction = async () => {
    setLoading(true);
    onShowToast('Procesando...', 'info');
    addLog('info', `Ejecutando ${modalAction}...`);

    try {
      const endpoint = getEndpoint();

      if (!endpoint) {
        throw new Error('Acción no reconocida');
      }

      const response = await fetch(`${API_URL}${endpoint}`, {
        method: 'POST',
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || 'Error ejecutando la acción');
      }

      if (modalAction === 'Web Scraping') {
        setLastScraping('Ahora');
        addLog(
          'success',
          `Web scraping completado. Registros: ${data.registros_obtenidos ?? 0}`
        );
      }

      if (modalAction === 'Actualización de Clima') {
        setLastWeather('Ahora');
        addLog(
          'success',
          `Clima actualizado. Registros: ${data.registros_obtenidos ?? 0}`
        );
      }

      if (modalAction === 'Entrenamiento de Modelo') {
        setLastTraining('Ahora');
        setModelName(data.modelo_nombre || 'Random Forest');
        setAccuracy(data.accuracy !== undefined ? `${data.accuracy}%` : 'Entrenado');
        addLog(
          'success',
          data.mensaje || 'Modelo entrenado correctamente'
        );
      }

      onShowToast(data.mensaje || `${modalAction} completado correctamente`, 'success');
      setShowModal(false);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Error inesperado';

      onShowToast(message, 'error');
      addLog('error', `${modalAction} falló: ${message}`);
    } finally {
      setLoading(false);
    }
  };

  const stats = [
    {
      label: 'Último Scraping',
      value: lastScraping,
      icon: Globe,
      color: lastScraping === 'Pendiente' ? 'default' : 'success',
    },
    {
      label: 'Último Entrenamiento',
      value: lastTraining,
      icon: Brain,
      color: lastTraining === 'Pendiente' ? 'default' : 'success',
    },
    {
      label: 'Modelo Activo',
      value: modelName,
      icon: Brain,
      color: 'default',
    },
    {
      label: 'Precisión del Modelo',
      value: accuracy,
      icon: CheckCircle2,
      color: accuracy === 'Pendiente' ? 'default' : 'success',
    },
  ];

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {stats.map((stat) => {
          const Icon = stat.icon;

          return (
            <Card key={stat.label}>
              <CardContent className="p-6">
                <div className="flex items-start justify-between mb-2">
                  <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
                    <Icon className="h-5 w-5 text-primary" />
                  </div>

                  {stat.color === 'success' && (
                    <CheckCircle2 className="h-4 w-4 text-success" />
                  )}
                </div>

                <p className="text-sm text-muted-foreground mb-1">
                  {stat.label}
                </p>

                <p className="text-lg">{stat.value}</p>
              </CardContent>
            </Card>
          );
        })}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Globe className="h-5 w-5" />
              Web Scraping
            </CardTitle>
          </CardHeader>

          <CardContent className="space-y-3">
            <div className="p-3 bg-muted rounded-lg">
              <p className="text-sm text-muted-foreground">Estado</p>
              <Badge variant="success" className="mt-1">
                Disponible
              </Badge>
              <p className="text-xs text-muted-foreground mt-2">
                Última ejecución: {lastScraping}
              </p>
            </div>

            <Button
              variant="primary"
              className="w-full"
              onClick={() => handleAction('Web Scraping')}
              disabled={loading}
            >
              <Play className="h-4 w-4" />
              Ejecutar Scraping
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Database className="h-5 w-5" />
              Datos Clima
            </CardTitle>
          </CardHeader>

          <CardContent className="space-y-3">
            <div className="p-3 bg-muted rounded-lg">
              <p className="text-sm text-muted-foreground">Estado</p>
              <Badge variant="success" className="mt-1">
                Disponible
              </Badge>
              <p className="text-xs text-muted-foreground mt-2">
                Última actualización: {lastWeather}
              </p>
            </div>

            <Button
              variant="primary"
              className="w-full"
              onClick={() => handleAction('Actualización de Clima')}
              disabled={loading}
            >
              <Play className="h-4 w-4" />
              Actualizar Clima
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base flex items-center gap-2">
              <Brain className="h-5 w-5" />
              Entrenamiento ML
            </CardTitle>
          </CardHeader>

          <CardContent className="space-y-3">
            <div className="p-3 bg-muted rounded-lg">
              <p className="text-sm text-muted-foreground">Modelo</p>
              <p className="text-sm mt-1">{modelName}</p>
              <p className="text-xs text-muted-foreground mt-2">
                Precisión: {accuracy}
              </p>
            </div>

            <Button
              variant="primary"
              className="w-full"
              onClick={() => handleAction('Entrenamiento de Modelo')}
              disabled={loading}
            >
              <Play className="h-4 w-4" />
              Entrenar Modelo
            </Button>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Logs Recientes del Sistema</CardTitle>
        </CardHeader>

        <CardContent>
          <div className="space-y-2">
            {logs.map((log, idx) => (
              <div
                key={idx}
                className="flex items-start gap-3 p-3 bg-muted rounded-lg text-sm"
              >
                <Clock className="h-4 w-4 text-muted-foreground mt-0.5 flex-shrink-0" />
                <span className="text-muted-foreground min-w-[70px]">
                  {log.time}
                </span>

                <div className="flex-1">
                  {log.type === 'success' && (
                    <CheckCircle2 className="h-4 w-4 text-success inline mr-2" />
                  )}

                  {log.type === 'error' && (
                    <AlertCircle className="h-4 w-4 text-destructive inline mr-2" />
                  )}

                  {log.type === 'info' && (
                    <Database className="h-4 w-4 text-primary inline mr-2" />
                  )}

                  <span>{log.message}</span>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <Modal
        isOpen={showModal}
        onClose={() => setShowModal(false)}
        title="Confirmar Acción"
        onConfirm={confirmAction}
        confirmText="Ejecutar"
        confirmVariant="primary"
        loading={loading}
      >
        <p className="text-sm text-muted-foreground">
          ¿Estás seguro de que deseas ejecutar <strong>{modalAction}</strong>?
        </p>

        <p className="text-sm text-muted-foreground mt-2">
          Esta acción se ejecutará realmente contra el backend de FastAPI.
        </p>
      </Modal>
    </div>
  );
}