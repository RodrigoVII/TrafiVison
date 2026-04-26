import { useEffect, useState } from 'react';
import { Camera, Clock, Thermometer, TrendingUp, AlertTriangle } from 'lucide-react';
import { KPICard } from '../ui/KPICard';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Badge } from '../ui/Badge';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const API_URL = 'http://localhost:8000';

interface DashboardData {
  camaras_activas: number;
  total_capturas: number;
  ultima_actualizacion: string;
  temperatura_actual: number;
  prediccion_proxima_hora: string;
}

interface CameraData {
  id: number;
  codigo: string;
  nombre: string;
  estado: string;
}

interface HistoricoData {
  timestamp: string;
  num_vehiculos: number | null;
}

interface TrafficChartData {
  time: string;
  nivel: number;
}

export function Dashboard() {
  const [dashboard, setDashboard] = useState<DashboardData | null>(null);
  const [cameras, setCameras] = useState<CameraData[]>([]);
  const [trafficData, setTrafficData] = useState<TrafficChartData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    async function cargarDatos() {
      try {
        setLoading(true);

        const [dashboardResponse, camerasResponse, historicoResponse] = await Promise.all([
          fetch(`${API_URL}/api/dashboard`),
          fetch(`${API_URL}/api/camaras`),
          fetch(`${API_URL}/api/historico?limit=200`),
        ]);

        if (!dashboardResponse.ok) throw new Error('No se pudo cargar el dashboard');
        if (!camerasResponse.ok) throw new Error('No se pudieron cargar las cámaras');
        if (!historicoResponse.ok) throw new Error('No se pudo cargar el histórico');

        const dashboardData = await dashboardResponse.json();
        const camerasData = await camerasResponse.json();
        const historicoData: HistoricoData[] = await historicoResponse.json();

        setDashboard(dashboardData);
        setCameras(camerasData.slice(0, 4));

        // Convertimos el histórico real de la BD en datos para la gráfica.
        // Agrupamos por hora y usamos la media de vehículos detectados.
        const agrupadoPorHora: Record<string, number[]> = {};

        historicoData.forEach((item) => {
          if (!item.timestamp || item.num_vehiculos === null) return;

          const fecha = new Date(item.timestamp);
          const hora = `${String(fecha.getHours()).padStart(2, '0')}:00`;

          if (!agrupadoPorHora[hora]) agrupadoPorHora[hora] = [];
          agrupadoPorHora[hora].push(item.num_vehiculos);
        });

        const grafica = Object.entries(agrupadoPorHora)
          .map(([hora, valores]) => ({
            time: hora,
            nivel: Math.round(valores.reduce((acc, value) => acc + value, 0) / valores.length),
          }))
          .sort((a, b) => a.time.localeCompare(b.time));

        setTrafficData(grafica);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error cargando datos');
      } finally {
        setLoading(false);
      }
    }

    cargarDatos();
  }, []);

  if (loading) {
    return <p className="text-muted-foreground">Cargando datos reales de TrafiVision...</p>;
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
      <div>
        <h2 className="text-2xl mb-2">Bienvenido a TrafiVision</h2>
        <p className="text-muted-foreground">Estado actual del tráfico en Madrid</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard title="Cámaras Activas" value={`${dashboard?.camaras_activas ?? 0}`} icon={Camera} color="success" />
        <KPICard title="Capturas Registradas" value={`${dashboard?.total_capturas ?? 0}`} icon={Clock} color="primary" />
        <KPICard title="Temperatura Actual" value={`${dashboard?.temperatura_actual ?? 0}°C`} icon={Thermometer} color="primary" />
        <KPICard title="Predicción Próxima Hora" value={dashboard?.prediccion_proxima_hora ?? 'Sin datos'} icon={TrendingUp} color="warning" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle>Evolución del Tráfico desde la base de datos</CardTitle>
          </CardHeader>
          <CardContent>
            <ResponsiveContainer width="100%" height={300}>
              <LineChart data={trafficData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis dataKey="time" stroke="#64748b" />
                <YAxis stroke="#64748b" />
                <Tooltip />
                <Line type="monotone" dataKey="nivel" stroke="#0066ff" strokeWidth={2} />
              </LineChart>
            </ResponsiveContainer>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Alertas Recientes</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="flex items-start gap-3 p-3 bg-muted rounded-lg">
              <AlertTriangle className="h-4 w-4 mt-0.5 text-success" />
              <div>
                <p className="text-sm">Dashboard conectado a MariaDB</p>
                <p className="text-xs text-muted-foreground mt-1">
                  Última actualización: {dashboard?.ultima_actualizacion}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Estado de Cámaras</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {cameras.map((camera) => (
              <div key={camera.id} className="p-4 bg-muted rounded-lg">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-sm">{camera.codigo}</span>
                  <Badge variant={camera.estado === 'activa' ? 'success' : 'destructive'}>
                    {camera.estado === 'activa' ? 'Activa' : 'Sin datos'}
                  </Badge>
                </div>
                <p className="text-sm mb-2">{camera.nombre}</p>
                <span className="text-xs text-muted-foreground">Datos desde MariaDB</span>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}