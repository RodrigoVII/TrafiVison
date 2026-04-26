import { useEffect, useState } from 'react';
import { Calendar, Clock, Camera, TrendingUp, AlertCircle } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Button } from '../ui/Button';
import { Badge } from '../ui/Badge';

const API_URL = 'http://localhost:8000';

interface CameraData {
  id: number;
  codigo: string;
  nombre: string;
}

export function PredictionPage() {
  // Estados principales del formulario.
  const [selectedDate, setSelectedDate] = useState('2026-04-25');
  const [selectedTime, setSelectedTime] = useState('18:00');

  // Aquí guardo la calle seleccionada. Ahora viene desde la base de datos.
  const [selectedCamera, setSelectedCamera] = useState('');

  // Lista de calles/cámaras cargadas desde MariaDB.
  const [calles, setCalles] = useState<CameraData[]>([]);

  // Estos campos son los que realmente se mandan al modelo predictivo.
  const [franja, setFranja] = useState('tarde');
  const [laborable, setLaborable] = useState('si');
  const [lluvia, setLluvia] = useState('no');
  const [temperatura, setTemperatura] = useState(22);
  const [modelo, setModelo] = useState('random_forest');

  // Resultado de la predicción.
  const [prediction, setPrediction] = useState<any>(null);

  // Estados de control para carga y errores.
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  // Cargo las calles reales desde el backend para no escribirlas a mano.
  useEffect(() => {
    async function cargarCalles() {
      try {
        const response = await fetch(`${API_URL}/api/camaras`);

        if (!response.ok) {
          throw new Error('No se pudieron cargar las calles');
        }

        const data = await response.json();

        setCalles(data);

        // Si hay cámaras en la base de datos, dejo seleccionada la primera por defecto.
        if (data.length > 0) {
          setSelectedCamera(data[0].nombre);
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error cargando calles');
      }
    }

    cargarCalles();
  }, []);

  const handlePredict = async () => {
    setLoading(true);
    setError('');
    setPrediction(null);

    try {
      // Construyo el JSON con los mismos nombres que espera el backend FastAPI.
      const response = await fetch(`${API_URL}/api/predict`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          calle: selectedCamera,
          franja_horaria: franja,
          laborable: laborable,
          lluvia_cat: lluvia,
          temperatura: temperatura,
          modelo: modelo,
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || 'Error al calcular la predicción');
      }

      setPrediction(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error inesperado');
    } finally {
      setLoading(false);
    }
  };

  const canPredict = selectedDate && selectedTime && selectedCamera;

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle>Nueva Predicción</CardTitle>
        </CardHeader>

        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
            <div>
              <label className="text-sm mb-2 flex items-center gap-2">
                <Calendar className="h-4 w-4" /> Fecha
              </label>
              <input
                type="date"
                value={selectedDate}
                onChange={(e) => setSelectedDate(e.target.value)}
                className="w-full px-4 py-2 border rounded-lg"
              />
            </div>

            <div>
              <label className="text-sm mb-2 flex items-center gap-2">
                <Clock className="h-4 w-4" /> Hora
              </label>
              <input
                type="time"
                value={selectedTime}
                onChange={(e) => setSelectedTime(e.target.value)}
                className="w-full px-4 py-2 border rounded-lg"
              />
            </div>

            <div>
              <label className="text-sm mb-2 flex items-center gap-2">
                <Camera className="h-4 w-4" /> Calle
              </label>

              <select
                value={selectedCamera}
                onChange={(e) => setSelectedCamera(e.target.value)}
                className="w-full px-4 py-2 border rounded-lg"
              >
                <option value="">Selecciona una calle</option>

                {calles.map((calle) => (
                  <option key={calle.id} value={calle.nombre}>
                    {calle.nombre}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="text-sm mb-2">Franja horaria</label>
              <select
                value={franja}
                onChange={(e) => setFranja(e.target.value)}
                className="w-full px-4 py-2 border rounded-lg"
              >
                <option value="mañana">Mañana</option>
                <option value="tarde">Tarde</option>
                <option value="noche">Noche</option>
              </select>
            </div>

            <div>
              <label className="text-sm mb-2">Laborable</label>
              <select
                value={laborable}
                onChange={(e) => setLaborable(e.target.value)}
                className="w-full px-4 py-2 border rounded-lg"
              >
                <option value="si">Sí</option>
                <option value="no">No</option>
              </select>
            </div>

            <div>
              <label className="text-sm mb-2">Lluvia</label>
              <select
                value={lluvia}
                onChange={(e) => setLluvia(e.target.value)}
                className="w-full px-4 py-2 border rounded-lg"
              >
                <option value="no">No</option>
                <option value="ligera">Ligera</option>
                <option value="fuerte">Fuerte</option>
              </select>
            </div>

            <div>
              <label className="text-sm mb-2">Temperatura</label>
              <input
                type="number"
                value={temperatura}
                onChange={(e) => setTemperatura(Number(e.target.value))}
                className="w-full px-4 py-2 border rounded-lg"
              />
            </div>

            <div>
              <label className="text-sm mb-2">Modelo</label>
              <select
                value={modelo}
                onChange={(e) => setModelo(e.target.value)}
                className="w-full px-4 py-2 border rounded-lg"
              >
                <option value="random_forest">Random Forest</option>
                <option value="decision_tree">Decision Tree</option>
                <option value="logistic_regression">Logistic Regression</option>
                <option value="knn">KNN</option>
              </select>
            </div>
          </div>

          <Button onClick={handlePredict} loading={loading} disabled={!canPredict}>
            <TrendingUp className="h-4 w-4" />
            {loading ? 'Calculando...' : 'Calcular Predicción'}
          </Button>

          {error && (
            <div className="flex items-center gap-2 text-sm text-destructive mt-3">
              <AlertCircle className="h-4 w-4" />
              {error}
            </div>
          )}
        </CardContent>
      </Card>

      {prediction && (
        <Card>
          <CardHeader>
            <CardTitle>Resultado</CardTitle>
          </CardHeader>

          <CardContent>
            <div className="space-y-4">
              <div>
                <span className="text-sm text-muted-foreground mr-2">Nivel de tráfico:</span>
                <Badge>{prediction.nivel_trafico}</Badge>
              </div>

              <div>
                <span className="text-sm text-muted-foreground">Modelo:</span>
                <p>{prediction.modelo}</p>
              </div>

              {prediction.probabilidades && (
                <div>
                  <span className="text-sm text-muted-foreground">Probabilidades:</span>
                  <pre className="text-xs bg-muted p-3 rounded-lg mt-2 overflow-x-auto">
                    {JSON.stringify(prediction.probabilidades, null, 2)}
                  </pre>
                </div>
              )}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}