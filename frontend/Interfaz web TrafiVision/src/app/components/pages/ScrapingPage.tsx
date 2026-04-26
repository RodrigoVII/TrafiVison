import { useState } from 'react';
import { Globe, Database, Play, CheckCircle2, XCircle, Clock, AlertCircle } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Button } from '../ui/Button';
import { Badge } from '../ui/Badge';
import { Modal } from '../ui/Modal';
import { Alert } from '../ui/Alert';

const API_URL = 'http://localhost:8000';

export function ScrapingPage() {
  const [showModal, setShowModal] = useState(false);
  const [actionType, setActionType] = useState('');
  const [loading, setLoading] = useState(false);

  const [lastResult, setLastResult] = useState<any>(null);
  const [error, setError] = useState('');

  // Guardo logs en memoria para enseñar al usuario qué acciones ha ejecutado.
  const [logs, setLogs] = useState([
    {
      time: 'Sistema',
      type: 'info',
      message: 'Panel de datos preparado para ejecutar procesos administrativos',
    },
  ]);

  const handleExecute = (type: string) => {
    setActionType(type);
    setShowModal(true);
  };

  const confirmExecute = async () => {
    setLoading(true);
    setError('');
    setLastResult(null);

    try {
      // Para Fase 2 usamos el mismo endpoint de scraping como proceso de actualización.
      // Más adelante se podrían separar scraping, clima y ETL en endpoints diferentes.
      const response = await fetch(`${API_URL}/api/admin/scraping`, {
        method: 'POST',
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || 'Error al ejecutar el proceso');
      }

      setLastResult(data);

      setLogs((prev) => [
        {
          time: new Date().toLocaleTimeString(),
          type: 'success',
          message: `${actionType} completado: ${data.registros_obtenidos} registros obtenidos`,
        },
        ...prev,
      ]);

      setShowModal(false);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Error inesperado';

      setError(message);

      setLogs((prev) => [
        {
          time: new Date().toLocaleTimeString(),
          type: 'error',
          message: `${actionType} falló: ${message}`,
        },
        ...prev,
      ]);
    } finally {
      setLoading(false);
    }
  };

  const executions = [
    {
      id: 1,
      date: 'Última ejecución',
      type: lastResult ? actionType : 'Sin ejecución reciente',
      status: lastResult ? 'success' : 'pending',
      records: lastResult?.registros_obtenidos ?? 0,
      duration: 'Proceso inmediato',
      errors: 0,
    },
  ];

  return (
    <div className="space-y-6">
      {lastResult && (
        <Alert variant="success" onClose={() => setLastResult(null)}>
          {lastResult.mensaje}. Registros obtenidos: {lastResult.registros_obtenidos}
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

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="w-12 h-12 rounded-lg bg-primary/10 flex items-center justify-center">
                <Globe className="h-6 w-6 text-primary" />
              </div>
              <Badge variant="success">Activo</Badge>
            </div>

            <h3 className="text-sm mb-2">Web Scraping</h3>
            <p className="text-xs text-muted-foreground mb-4">
              Actualiza capturas y datos recogidos desde las fuentes del proyecto.
            </p>

            <Button
              variant="primary"
              size="sm"
              className="w-full"
              onClick={() => handleExecute('Web Scraping de Cámaras')}
            >
              <Play className="h-4 w-4" />
              Ejecutar Scraping
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="w-12 h-12 rounded-lg bg-success/10 flex items-center justify-center">
                <Database className="h-6 w-6 text-success" />
              </div>
              <Badge variant="success">Activo</Badge>
            </div>

            <h3 className="text-sm mb-2">Datos Clima</h3>
            <p className="text-xs text-muted-foreground mb-4">
              Prepara la actualización de datos meteorológicos asociados.
            </p>

            <Button
              variant="primary"
              size="sm"
              className="w-full"
              onClick={() => handleExecute('Actualización de Clima')}
            >
              <Play className="h-4 w-4" />
              Actualizar Clima
            </Button>
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="w-12 h-12 rounded-lg bg-warning/10 flex items-center justify-center">
                <Database className="h-6 w-6 text-warning" />
              </div>
              <Badge variant="warning">Manual</Badge>
            </div>

            <h3 className="text-sm mb-2">Proceso ETL</h3>
            <p className="text-xs text-muted-foreground mb-4">
              Ejecuta procesamiento y preparación de datos para el modelo.
            </p>

            <Button
              variant="secondary"
              size="sm"
              className="w-full"
              onClick={() => handleExecute('Procesamiento ETL')}
            >
              <Play className="h-4 w-4" />
              Procesar Datos
            </Button>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Estado de Procesos</CardTitle>
        </CardHeader>

        <CardContent>
          <div className="space-y-3">
            <div className="p-4 bg-muted rounded-lg">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm">Web Scraping</span>
                <Badge variant="success">Preparado</Badge>
              </div>
              <p className="text-xs text-muted-foreground">
                Proceso disponible para administrador. En Fase 2 queda conectado al backend.
              </p>
            </div>

            <div className="p-4 bg-muted rounded-lg">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm">Actualización de Clima</span>
                <Badge variant="success">Preparado</Badge>
              </div>
              <p className="text-xs text-muted-foreground">
                Se mantiene como acción administrativa para actualizar datos meteorológicos.
              </p>
            </div>

            <div className="p-4 bg-muted rounded-lg">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm">Proceso ETL</span>
                <Badge variant="warning">Manual</Badge>
              </div>
              <p className="text-xs text-muted-foreground">
                Limpieza y preparación de datos antes de entrenar modelos predictivos.
              </p>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Logs Recientes</CardTitle>
        </CardHeader>

        <CardContent>
          <div className="space-y-2 mb-4">
            {logs.map((log, idx) => (
              <div key={idx} className="flex items-start gap-3 p-3 bg-muted rounded-lg text-sm">
                <Clock className="h-4 w-4 text-muted-foreground mt-0.5" />
                <span className="text-muted-foreground min-w-[70px]">{log.time}</span>

                <div className="flex-1">
                  {log.type === 'success' && <CheckCircle2 className="h-4 w-4 text-success inline mr-2" />}
                  {log.type === 'error' && <XCircle className="h-4 w-4 text-destructive inline mr-2" />}
                  {log.type === 'info' && <Database className="h-4 w-4 text-primary inline mr-2" />}
                  <span>{log.message}</span>
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Historial de Ejecuciones</CardTitle>
        </CardHeader>

        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-3 px-4 text-sm">Fecha/Hora</th>
                  <th className="text-left py-3 px-4 text-sm">Tipo de Proceso</th>
                  <th className="text-left py-3 px-4 text-sm">Estado</th>
                  <th className="text-left py-3 px-4 text-sm">Registros</th>
                  <th className="text-left py-3 px-4 text-sm">Duración</th>
                  <th className="text-left py-3 px-4 text-sm">Errores</th>
                </tr>
              </thead>

              <tbody>
                {executions.map((exec) => (
                  <tr key={exec.id} className="border-b border-border last:border-0 hover:bg-muted/50">
                    <td className="py-3 px-4 text-sm">{exec.date}</td>
                    <td className="py-3 px-4 text-sm">{exec.type}</td>
                    <td className="py-3 px-4">
                      <Badge variant={exec.status === 'success' ? 'success' : 'secondary'}>
                        {exec.status === 'success' ? 'Exitoso' : 'Pendiente'}
                      </Badge>
                    </td>
                    <td className="py-3 px-4 text-sm">{exec.records.toLocaleString()}</td>
                    <td className="py-3 px-4 text-sm">{exec.duration}</td>
                    <td className="py-3 px-4 text-sm">{exec.errors}</td>
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
        title="Confirmar Ejecución"
        onConfirm={confirmExecute}
        confirmText="Ejecutar"
        confirmVariant="primary"
        loading={loading}
      >
        <p className="text-sm text-muted-foreground">
          ¿Seguro que quieres ejecutar <strong>{actionType}</strong>?
        </p>

        <p className="text-sm text-muted-foreground mt-2">
          Esta acción solo está disponible para el administrador.
        </p>
      </Modal>
    </div>
  );
}