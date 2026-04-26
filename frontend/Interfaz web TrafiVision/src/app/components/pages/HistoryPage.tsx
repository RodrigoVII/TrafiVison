import { useEffect, useMemo, useState } from 'react';
import { Download, Filter, X } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Button } from '../ui/Button';
import { Badge } from '../ui/Badge';
import { EmptyState } from '../ui/EmptyState';
import { LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

const API_URL = 'http://localhost:8000';

interface HistoryRecord {
  timestamp: string;
  camara: string;
  franja_horaria: string;
  laborable: boolean | null;
  num_vehiculos: number | null;
  nivel_trafico: string | null;
  temperatura: number | null;
  precipitacion: number | null;
}

export function HistoryPage() {
  const [records, setRecords] = useState<HistoryRecord[]>([]);
  const [dateFrom, setDateFrom] = useState('');
  const [dateTo, setDateTo] = useState('');
  const [selectedCamera, setSelectedCamera] = useState('all');
  const [trafficLevel, setTrafficLevel] = useState('all');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  // 🔥 AHORA CARGA TODOS LOS DATOS (sin límite real)
  useEffect(() => {
    async function cargarHistorico() {
      try {
        setLoading(true);

        const response = await fetch(`${API_URL}/api/historico?limit=100000`);

        if (!response.ok) {
          throw new Error('No se pudo cargar el histórico');
        }

        const data = await response.json();
        setRecords(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error cargando histórico');
      } finally {
        setLoading(false);
      }
    }

    cargarHistorico();
  }, []);

  const cameras = useMemo(() => {
    return Array.from(new Set(records.map((record) => record.camara))).filter(Boolean);
  }, [records]);

  const filteredRecords = useMemo(() => {
    return records.filter((record) => {
      const fecha = record.timestamp.split(' ')[0] || record.timestamp.split('T')[0];

      const matchesDateFrom = !dateFrom || fecha >= dateFrom;
      const matchesDateTo = !dateTo || fecha <= dateTo;
      const matchesCamera = selectedCamera === 'all' || record.camara === selectedCamera;
      const matchesTraffic =
        trafficLevel === 'all' ||
        (record.nivel_trafico || '').toLowerCase() === trafficLevel.toLowerCase();

      return matchesDateFrom && matchesDateTo && matchesCamera && matchesTraffic;
    });
  }, [records, dateFrom, dateTo, selectedCamera, trafficLevel]);

  const hasFilters = Boolean(dateFrom || dateTo || selectedCamera !== 'all' || trafficLevel !== 'all');

  const clearFilters = () => {
    setDateFrom('');
    setDateTo('');
    setSelectedCamera('all');
    setTrafficLevel('all');
  };

  const evolutionData = useMemo(() => {
    const grouped: Record<string, number[]> = {};

    filteredRecords.forEach((record) => {
      if (record.num_vehiculos === null) return;

      const fecha = new Date(record.timestamp);
      const hora = `${String(fecha.getHours()).padStart(2, '0')}:00`;

      if (!grouped[hora]) grouped[hora] = [];
      grouped[hora].push(record.num_vehiculos);
    });

    return Object.entries(grouped)
      .map(([hora, values]) => ({
        hora,
        trafico: Math.round(values.reduce((acc, value) => acc + value, 0) / values.length),
      }))
      .sort((a, b) => a.hora.localeCompare(b.hora));
  }, [filteredRecords]);

  const comparisonData = useMemo(() => {
    const grouped: Record<string, number[]> = {};

    filteredRecords.forEach((record) => {
      if (record.num_vehiculos === null) return;

      const fecha = record.timestamp.split(' ')[0] || record.timestamp.split('T')[0];

      if (!grouped[fecha]) grouped[fecha] = [];
      grouped[fecha].push(record.num_vehiculos);
    });

    return Object.entries(grouped)
      .map(([dia, values]) => ({
        dia,
        trafico: Math.round(values.reduce((acc, value) => acc + value, 0) / values.length),
      }));
  }, [filteredRecords]);

  const weatherData = useMemo(() => {
    const grouped: Record<string, number[]> = {
      'Sin lluvia': [],
      'Con lluvia': [],
    };

    filteredRecords.forEach((record) => {
      if (record.num_vehiculos === null) return;

      const clima = record.precipitacion && record.precipitacion > 0 ? 'Con lluvia' : 'Sin lluvia';
      grouped[clima].push(record.num_vehiculos);
    });

    return Object.entries(grouped)
      .filter(([, values]) => values.length > 0)
      .map(([clima, values]) => ({
        clima,
        traficoPromedio: Math.round(values.reduce((acc, value) => acc + value, 0) / values.length),
      }));
  }, [filteredRecords]);

  const getLevelBadge = (level: string | null) => {
    const normalized = (level || 'desconocido').toLowerCase();

    if (normalized === 'alto') return 'destructive';
    if (normalized === 'medio') return 'warning';
    if (normalized === 'bajo') return 'success';

    return 'secondary';
  };

  const exportData = () => {
    const csvHeader = 'timestamp,camara,franja_horaria,laborable,num_vehiculos,nivel_trafico,temperatura,precipitacion\n';

    const csvRows = filteredRecords
      .map((record) =>
        [
          record.timestamp,
          record.camara,
          record.franja_horaria,
          record.laborable,
          record.num_vehiculos,
          record.nivel_trafico,
          record.temperatura,
          record.precipitacion,
        ].join(',')
      )
      .join('\n');

    const blob = new Blob([csvHeader + csvRows], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);

    const link = document.createElement('a');
    link.href = url;
    link.download = 'trafivision_historico.csv';
    link.click();

    URL.revokeObjectURL(url);
  };

  if (loading) return <p>Cargando histórico...</p>;

  if (error) return <p>{error}</p>;

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Registros Históricos</CardTitle>
        </CardHeader>

        <CardContent>
          <div className="overflow-x-auto max-h-[600px] overflow-y-auto">
            <table className="w-full">
              <thead>
                <tr>
                  <th>Fecha/Hora</th>
                  <th>Cámara</th>
                  <th>Franja</th>
                  <th>Vehículos</th>
                  <th>Nivel</th>
                  <th>Temp</th>
                  <th>Lluvia</th>
                </tr>
              </thead>

              <tbody>
                {/* 🔥 SIN LIMITES */}
                {filteredRecords.map((record, idx) => (
                  <tr key={idx}>
                    <td>{record.timestamp}</td>
                    <td>{record.camara}</td>
                    <td>{record.franja_horaria}</td>
                    <td>{record.num_vehiculos ?? '-'}</td>
                    <td>
                      <Badge variant={getLevelBadge(record.nivel_trafico) as any}>
                        {record.nivel_trafico ?? '-'}
                      </Badge>
                    </td>
                    <td>{record.temperatura ?? '-'}</td>
                    <td>{record.precipitacion ?? '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}