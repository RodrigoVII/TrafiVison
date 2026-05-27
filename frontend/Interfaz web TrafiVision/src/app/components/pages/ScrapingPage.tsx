import { useEffect, useState } from 'react';
import { Globe, Database, Play, Square, CheckCircle2, XCircle, Clock, AlertCircle } from 'lucide-react';
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

  const [scrapingActivo, setScrapingActivo] = useState(false);
  const [lastResult, setLastResult] = useState<any>(null);
  const [error, setError] = useState('');

  const [logs, setLogs] = useState([
    {
      time: 'Sistema',
      type: 'info',
      message: 'Panel preparado para iniciar scraping, clima y ETL',
    },
  ]);

  // Compruebo al cargar la página si el scraping continuo está activo o parado.
  useEffect(() => {
    comprobarEstadoScraping();
  }, []);

  const addLog = (type: string, message: string) => {
    setLogs((prev) => [
      {
        time: new Date().toLocaleTimeString(),
        type,
        message,
      },
      ...prev,
    ]);
  };

  const comprobarEstadoScraping = async () => {
    try {
      const response = await fetch(`${API_URL}/api/admin/scraping/status`);
      const data = await response.json();

      if (response.ok) {
        setScrapingActivo(data.activo);
      }
    } catch {
      // Si falla la consulta de estado, no bloqueo la pantalla.
    }
  };

  const handleExecute = (type: string) => {
    setActionType(type);
    setShowModal(true);
  };

  const getEndpoint = () => {
    if (actionType === 'Iniciar Scraping Continuo') {
      return '/api/admin/scraping/start';
    }

    if (actionType === 'Parar Scraping Continuo') {
      return '/api/admin/scraping/stop';
    }

    if (actionType === 'Actualización de Clima') {
      return '/api/admin/clima';
    }

    if (actionType === 'Procesamiento ETL') {
      return '/api/admin/etl';
    }

    return '/api/admin/scraping';
  };

  const confirmExecute = async () => {
    setLoading(true);
    setError('');
    setLastResult(null);

    try {
      const response = await fetch(`${API_URL}${getEndpoint()}`, {
        method: 'POST',
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || 'Error al ejecutar el proceso');
      }

      setLastResult(data);

      if (actionType === 'Iniciar Scraping Continuo') {
        setScrapingActivo(true);
        addLog('success', 'Scraping continuo iniciado. Capturará imágenes cada 15 minutos.');
      } else if (actionType === 'Parar Scraping Continuo') {
        setScrapingActivo(false);
        addLog('success', 'Scraping continuo detenido por el administrador.');
      } else {
        addLog(
          'success',
          `${actionType} completado: ${data.registros_obtenidos ?? 0} registros obtenidos`
        );
      }

      setShowModal(false);
      comprobarEstadoScraping();
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Error inesperado';
      setError(message);
      addLog('error', `${actionType} falló: ${message}`);
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
      duration: scrapingActivo ? 'Continuo cada 15 min' : 'Proceso inmediato',
      errors: 0,
    },
  ];

  return (
    <div className="space-y-6">
      {lastResult && (
        <Alert variant="success" onClose={() => setLastResult(null)}>
          {lastResult.mensaje}
          {lastResult.registros_obtenidos !== undefined && (
            <>. Registros obtenidos: {lastResult.registros_obtenidos}</>
          )}
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

              <Badge variant={scrapingActivo ? 'success' : 'secondary'}>
                {scrapingActivo ? 'Activo' : 'Parado'}
              </Badge>
            </div>

            <h3 className="text-sm mb-2">Web Scraping</h3>
            <p className="text-xs text-muted-foreground mb-4">
              Captura imágenes de cámaras cada 15 minutos hasta que se pulse parar.
            </p>

            {scrapingActivo ? (
              <Button
                variant="destructive"
                size="sm"
                className="w-full"
                onClick={() => handleExecute('Parar Scraping Continuo')}
              >
                <Square className="h-4 w-4" />
                Parar Scraping
              </Button>
            ) : (
              <Button
                variant="primary"
                size="sm"
                className="w-full"
                onClick={() => handleExecute('Iniciar Scraping Continuo')}
              >
                <Play className="h-4 w-4" />
                Iniciar Scraping
              </Button>
            )}
          </CardContent>
        </Card>

        <Card>
          <CardContent className="p-6">
            <div className="flex items-center justify-between mb-4">
              <div className="w-12 h-12 rounded-lg bg-success/10 flex items-center justify-center">
                <Database className="h-6 w-6 text-success" />
              </div>
              <Badge variant="success">Manual</Badge>
            </div>

            <h3 className="text-sm mb-2">Datos Clima</h3>
            <p className="text-xs text-muted-foreground mb-4">
              Ejecuta una actualización real de clima con el script etl_tiempo.py.
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
              Proceso reservado para juntar cámaras, clima y cargar datos en MariaDB.
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
                <span className="text-sm">Web Scraping Continuo</span>
                <Badge variant={scrapingActivo ? 'success' : 'secondary'}>
                  {scrapingActivo ? 'Activo' : 'Parado'}
                </Badge>
              </div>
              <p className="text-xs text-muted-foreground">
                {scrapingActivo
                  ? 'Capturando imágenes automáticamente cada 15 minutos.'
                  : 'Scraping detenido. Pulsa iniciar para comenzar las capturas automáticas.'}
              </p>
            </div>

            <div className="p-4 bg-muted rounded-lg">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm">Actualización de Clima</span>
                <Badge variant="success">Disponible</Badge>
              </div>
              <p className="text-xs text-muted-foreground">
                Acción manual para guardar una nueva medición meteorológica.
              </p>
            </div>

            <div className="p-4 bg-muted rounded-lg">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm">Proceso ETL</span>
                <Badge variant="warning">Pendiente</Badge>
              </div>
              <p className="text-xs text-muted-foreground">
                Falta conectar el merge final y carga a base de datos.
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
        confirmText={
          actionType === 'Parar Scraping Continuo'
            ? 'Parar'
            : 'Ejecutar'
        }
        confirmVariant={
          actionType === 'Parar Scraping Continuo'
            ? 'destructive'
            : 'primary'
        }
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