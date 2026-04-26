import { useState } from 'react';
import { Brain, Play, CheckCircle2, TrendingUp, AlertCircle } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Button } from '../ui/Button';
import { Badge } from '../ui/Badge';
import { Modal } from '../ui/Modal';
import { Alert } from '../ui/Alert';

const API_URL = 'http://localhost:8000';

export function TrainingPage() {
  const [showModal, setShowModal] = useState(false);
  const [training, setTraining] = useState(false);
  const [progress, setProgress] = useState(0);
  const [selectedModel, setSelectedModel] = useState('random_forest');
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState('');

  const getModelName = (model: string) => {
    const names: Record<string, string> = {
      random_forest: 'Random Forest',
      decision_tree: 'Decision Tree',
      logistic_regression: 'Logistic Regression',
      knn: 'KNN',
    };

    return names[model] || model;
  };

  const handleTrain = async () => {
    setShowModal(false);
    setTraining(true);
    setProgress(0);
    setError('');
    setResult(null);

    // Uso una barra visual mientras FastAPI entrena los modelos.
    // El entrenamiento real ocurre en el backend.
    const interval = setInterval(() => {
      setProgress((prev) => (prev >= 90 ? 90 : prev + 10));
    }, 700);

    try {
      // Aquí llamo al endpoint real de entrenamiento.
      // Le paso el modelo seleccionado para que el backend sepa cuál mostrar como activo.
      const response = await fetch(`${API_URL}/api/admin/train?modelo=${selectedModel}`, {
        method: 'POST',
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || 'Error al entrenar el modelo');
      }

      setProgress(100);
      setResult(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Error inesperado');
      setProgress(0);
    } finally {
      clearInterval(interval);
      setTraining(false);
    }
  };

  const trainings = [
    {
      id: 1,
      date: result?.fecha || 'Última ejecución',
      model: result?.modelo_nombre || getModelName(selectedModel),
      dataset: 'Datos desde MariaDB',
      accuracy: result?.accuracy ?? '-',
      rmse: result?.rmse ?? '-',
      status: result?.estado ?? 'pendiente',
    },
  ];

  return (
    <div className="space-y-6">
      {training && (
        <Alert variant="info">
          <div className="space-y-2 w-full">
            <p>Entrenamiento en progreso... {progress}%</p>
            <div className="w-full h-2 bg-secondary rounded-full overflow-hidden">
              <div
                className="h-full bg-primary transition-all duration-300"
                style={{ width: `${progress}%` }}
              />
            </div>
          </div>
        </Alert>
      )}

      {result && !training && (
        <Alert variant="success" onClose={() => setResult(null)}>
          {result.mensaje}
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

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Brain className="h-5 w-5" />
            Entrenamiento de Modelo
          </CardTitle>
        </CardHeader>

        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="space-y-4">
              <div>
                <label className="block text-sm mb-2">Dataset</label>
                <select
                  disabled={training}
                  className="w-full px-4 py-2 bg-input-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring disabled:opacity-50"
                >
                  <option>Datos actuales de MariaDB</option>
                </select>
              </div>

              <div>
                <label className="block text-sm mb-2">Modelo activo</label>
                <select
                  value={selectedModel}
                  onChange={(e) => setSelectedModel(e.target.value)}
                  disabled={training}
                  className="w-full px-4 py-2 bg-input-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring disabled:opacity-50"
                >
                  <option value="random_forest">Random Forest</option>
                  <option value="decision_tree">Decision Tree</option>
                  <option value="logistic_regression">Logistic Regression</option>
                  <option value="knn">KNN</option>
                </select>
              </div>

              <div className="p-4 bg-muted rounded-lg space-y-2">
                <h4 className="text-sm">Información del entrenamiento</h4>

                <div className="space-y-2 text-sm">
                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Origen de datos:</span>
                    <span>MariaDB</span>
                  </div>

                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Script usado:</span>
                    <span>app/train_models.py</span>
                  </div>

                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Salida:</span>
                    <span>/models</span>
                  </div>

                  <div className="flex justify-between">
                    <span className="text-muted-foreground">Modelos generados:</span>
                    <span>4</span>
                  </div>
                </div>
              </div>

              <Button
                variant="primary"
                className="w-full"
                onClick={() => setShowModal(true)}
                disabled={training}
              >
                <Play className="h-4 w-4" />
                {training ? 'Entrenando...' : 'Entrenar Modelo'}
              </Button>
            </div>

            <div className="space-y-4">
              <Card className="border-primary/20">
                <CardHeader>
                  <CardTitle className="text-base">Modelo Activo Actual</CardTitle>
                </CardHeader>

                <CardContent className="space-y-3">
                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Algoritmo</span>
                    <Badge variant="success">
                      {result?.modelo_nombre || getModelName(selectedModel)}
                    </Badge>
                  </div>

                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Estado</span>
                    <Badge variant={result ? 'success' : 'secondary'}>
                      {result ? 'Entrenado' : 'Pendiente'}
                    </Badge>
                  </div>

                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">Accuracy</span>
                    <span className="text-sm">
                      {result?.accuracy !== undefined ? `${result.accuracy}%` : 'Pendiente'}
                    </span>
                  </div>

                  <div className="flex items-center justify-between">
                    <span className="text-sm text-muted-foreground">RMSE</span>
                    <span className="text-sm">
                      {result?.rmse !== undefined ? result.rmse : 'Pendiente'}
                    </span>
                  </div>
                </CardContent>
              </Card>

              <div className="p-4 bg-primary/5 border border-primary/20 rounded-lg">
                <div className="flex items-start gap-2">
                  <TrendingUp className="h-4 w-4 text-primary mt-0.5" />
                  <div>
                    <p className="text-sm">Recomendación</p>
                    <p className="text-xs text-muted-foreground mt-1">
                      Primero actualizo datos, después proceso el ETL y finalmente reentreno los modelos.
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle>Historial de Entrenamientos</CardTitle>
        </CardHeader>

        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="border-b border-border">
                  <th className="text-left py-3 px-4 text-sm">Fecha</th>
                  <th className="text-left py-3 px-4 text-sm">Modelo</th>
                  <th className="text-left py-3 px-4 text-sm">Dataset</th>
                  <th className="text-left py-3 px-4 text-sm">Accuracy</th>
                  <th className="text-left py-3 px-4 text-sm">RMSE</th>
                  <th className="text-left py-3 px-4 text-sm">Estado</th>
                </tr>
              </thead>

              <tbody>
                {trainings.map((item) => (
                  <tr key={item.id} className="border-b border-border last:border-0 hover:bg-muted/50">
                    <td className="py-3 px-4 text-sm">{item.date}</td>
                    <td className="py-3 px-4 text-sm">{item.model}</td>
                    <td className="py-3 px-4 text-sm">{item.dataset}</td>
                    <td className="py-3 px-4 text-sm">{item.accuracy}</td>
                    <td className="py-3 px-4 text-sm">{item.rmse}</td>
                    <td className="py-3 px-4">
                      <Badge variant={result ? 'success' : 'secondary'}>
                        <CheckCircle2 className="h-3 w-3 mr-1" />
                        {result ? 'Completado' : 'Pendiente'}
                      </Badge>
                    </td>
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
        title="Confirmar Entrenamiento"
        onConfirm={handleTrain}
        confirmText="Iniciar Entrenamiento"
        confirmVariant="primary"
      >
        <div className="space-y-3">
          <p className="text-sm text-muted-foreground">
            Voy a ejecutar el entrenamiento real usando <strong>app/train_models.py</strong>.
          </p>

          <div className="p-4 bg-muted rounded-lg space-y-2 text-sm">
            <div className="flex justify-between">
              <span className="text-muted-foreground">Dataset:</span>
              <span>MariaDB</span>
            </div>

            <div className="flex justify-between">
              <span className="text-muted-foreground">Modelo activo:</span>
              <span>{getModelName(selectedModel)}</span>
            </div>

            <div className="flex justify-between">
              <span className="text-muted-foreground">Salida:</span>
              <span>/models</span>
            </div>
          </div>

          <p className="text-sm text-muted-foreground">
            El proceso puede tardar unos segundos o minutos según los datos disponibles.
          </p>
        </div>
      </Modal>
    </div>
  );
}