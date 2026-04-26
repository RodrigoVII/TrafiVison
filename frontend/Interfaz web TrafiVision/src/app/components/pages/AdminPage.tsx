import { useEffect, useState } from 'react';
import { Database, Brain, Globe, CheckCircle2, AlertCircle, Clock } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Button } from '../ui/Button';
import { Badge } from '../ui/Badge';
import { Modal } from '../ui/Modal';
import { Alert } from '../ui/Alert';

const API_URL = 'http://localhost:8000';

export function AdminPage() {
  const [showModal, setShowModal] = useState(false);
  const [modalAction, setModalAction] = useState('');
  const [loading, setLoading] = useState(false);
  const [successMessage, setSuccessMessage] = useState('');
  const [error, setError] = useState('');
  const [dashboard, setDashboard] = useState<any>(null);

  const [logs, setLogs] = useState([
    { time: 'Sistema', type: 'info', message: 'Panel de administración iniciado correctamente' },
  ]);

  useEffect(() => {
    cargarDashboard();
  }, []);

  const cargarDashboard = async () => {
    try {
      const response = await fetch(`${API_URL}/api/dashboard`);

      if (!response.ok) {
        throw new Error('No se pudo cargar el resumen del sistema');
      }

      const data = await response.json();
      setDashboard(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error cargando dashboard');
    }
  };

  const handleAction = (action: string) => {
    setModalAction(action);
    setShowModal(true);
  };

  const confirmAction = async () => {
    setLoading(true);
    setError('');
    setSuccessMessage('');

    try {
      const endpoint =
        modalAction === 'Entrenamiento de Modelo'
          ? `${API_URL}/api/admin/train`
          : `${API_URL}/api/admin/scraping`;

      const response = await fetch(endpoint, {
        method: 'POST',
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || 'Error ejecutando acción');
      }

      const message = data.mensaje || `${modalAction} ejecutado correctamente`;

      setSuccessMessage(message);

      setLogs((prev) => [
        {
          time: new Date().toLocaleTimeString(),
          type: 'success',
          message,
        },
        ...prev,
      ]);

      setShowModal(false);
      await cargarDashboard();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Error inesperado';

      setError(message);

      setLogs((prev) => [
        {
          time: new Date().toLocaleTimeString(),
          type: 'warning',
          message,
        },
        ...prev,
      ]);
    } finally {
      setLoading(false);
    }
  };

  const stats = [
    {
      label: 'Cámaras activas',
      value: dashboard?.camaras_activas ?? '...',
      status: 'success',
    },
    {
      label: 'Capturas registradas',
      value: dashboard?.total_capturas ?? '...',
      status: 'success',
    },
    {
      label: 'Última actualización',
      value: dashboard?.ultima_actualizacion ?? '...',
      status: 'success',
    },
    {
      label: 'Modelo activo',
      value: dashboard?.modelo_activo ?? 'Random Forest',
      status: 'success',
    },
    {
      label: 'Precisión del modelo',
      value: `${dashboard?.accuracy ?? 87}%`,
      status: 'success',
    },
    {
      label: 'Predicción próxima hora',
      value: dashboard?.prediccion_proxima_hora ?? 'Sin datos',
      status: 'default',
    },
  ];

  return (
    <div className="space-y-6">
      {successMessage && (
        <Alert variant="success" onClose={() => setSuccessMessage('')}>
          {successMessage}
        </Alert>
      )}

      {error && (
        <Alert variant="error" onClose={() => setError('')}>
          {error}
        </Alert>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {stats.map((stat) => (
          <Card key={stat.label}>
            <CardContent className="p-6">
              <div className="flex items-start justify-between">
                <div>
                  <p className="text-sm text-muted-foreground mb-1">{stat.label}</p>
                  <p className="text-xl">{stat.value}</p>
                </div>

                {stat.status === 'success' && (
                  <CheckCircle2 className="h-5 w-5 text-success" />
                )}
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Database className="h-5 w-5" />
              Gestión de Datos
            </CardTitle>
          </CardHeader>

          <CardContent className="space-y-3">
            <Button
              variant="primary"
              className="w-full justify-start"
              onClick={() => handleAction('Web Scraping de Cámaras')}
            >
              <Globe className="h-4 w-4" />
              Ejecutar Web Scraping
            </Button>

            <Button
              variant="primary"
              className="w-full justify-start"
              onClick={() => handleAction('Actualización de Clima')}
            >
              <Database className="h-4 w-4" />
              Actualizar Datos Clima
            </Button>

            <Button
              variant="secondary"
              className="w-full justify-start"
              onClick={() => handleAction('Procesamiento ETL')}
            >
              <Database className="h-4 w-4" />
              Procesar / Limpiar Datos
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Brain className="h-5 w-5" />
              Machine Learning
            </CardTitle>
          </CardHeader>

          <CardContent className="space-y-3">
            <Button
              variant="primary"
              className="w-full justify-start"
              onClick={() => handleAction('Entrenamiento de Modelo')}
            >
              <Brain className="h-4 w-4" />
              Entrenar Modelo
            </Button>

            <Button
              variant="secondary"
              className="w-full justify-start"
              onClick={() => handleAction('Evaluación de Modelo')}
            >
              <CheckCircle2 className="h-4 w-4" />
              Evaluar Modelo Actual
            </Button>

            <div className="p-4 bg-muted rounded-lg">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm text-muted-foreground">Estado del Modelo</span>
                <Badge variant="success">Activo</Badge>
              </div>

              <div className="space-y-1 text-xs">
                <p className="flex justify-between">
                  <span className="text-muted-foreground">Accuracy:</span>
                  <span>{dashboard?.accuracy ?? 87}%</span>
                </p>

                <p className="flex justify-between">
                  <span className="text-muted-foreground">Modelo:</span>
                  <span>{dashboard?.modelo_activo ?? 'Random Forest'}</span>
                </p>

                <p className="flex justify-between">
                  <span className="text-muted-foreground">Datos:</span>
                  <span>MariaDB</span>
                </p>
              </div>
            </div>
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
              <div key={idx} className="flex items-start gap-3 p-3 bg-muted rounded-lg text-sm">
                <Clock className="h-4 w-4 text-muted-foreground mt-0.5" />
                <span className="text-muted-foreground min-w-[70px]">{log.time}</span>

                <div className="flex-1">
                  {log.type === 'success' && <CheckCircle2 className="h-4 w-4 text-success inline mr-2" />}
                  {log.type === 'warning' && <AlertCircle className="h-4 w-4 text-warning inline mr-2" />}
                  {log.type === 'info' && <AlertCircle className="h-4 w-4 text-primary inline mr-2" />}
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
          ¿Seguro que quieres ejecutar <strong>{modalAction}</strong>?
        </p>

        <p className="text-sm text-muted-foreground mt-2">
          Esta acción se lanzará desde el backend de TrafiVision.
        </p>
      </Modal>
    </div>
  );
}