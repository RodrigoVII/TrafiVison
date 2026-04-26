import { useState } from 'react';
import { Brain, Play, CheckCircle2, TrendingUp, AlertCircle } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Button } from '../ui/Button';
import { Badge } from '../ui/Badge';
import { Modal } from '../ui/Modal';
import { Alert } from '../ui/Alert';

const API_URL = 'http://localhost:8000';

interface TrainingPageSimplifiedProps {
  onShowToast: (message: string, type: 'success' | 'error' | 'info') => void;
}

export function TrainingPageSimplified({ onShowToast }: TrainingPageSimplifiedProps) {
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
    setResult(null);
    setError('');

    onShowToast('Ejecutando entrenamiento real...', 'info');

    const interval = setInterval(() => {
      setProgress((prev) => (prev >= 90 ? 90 : prev + 10));
    }, 700);

    try {
      const response = await fetch(`${API_URL}/api/admin/train?modelo=${selectedModel}`, {
        method: 'POST',
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || 'Error al entrenar el modelo');
      }

      setProgress(100);
      setResult(data);
      onShowToast(data.mensaje || 'Modelo entrenado correctamente', 'success');
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Error inesperado';
      setError(message);
      onShowToast(message, 'error');
      setProgress(0);
    } finally {
      clearInterval(interval);
      setTraining(false);
    }
  };

  const trainings = result
    ? [
        {
          id: 1,
          date: result.fecha || new Date().toLocaleString(),
          model: result.modelo_nombre || getModelName(selectedModel),
          accuracy: result.accuracy ?? 'Calculado',
          rmse: result.rmse ?? 'Calculado',
          duration: 'Real',
          status: 'Completado',
        },
      ]
    : [];

  return (
    <div className="space-y-6">
      {training && (
        <Card className="border-primary">
          <CardContent className="p-6">
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <p className="text-sm">Entrenamiento real en progreso...</p>
                <span className="text-sm text-primary">{progress}%</span>
              </div>

              <div className="w-full h-2 bg-secondary rounded-full overflow-hidden">
                <div
                  className="h-full bg-primary transition-all duration-300"
                  style={{ width: `${progress}%` }}
                />
              </div>

              <p className="text-xs text-muted-foreground">
                Se está ejecutando app/train_models.py desde FastAPI.
              </p>
            </div>
          </CardContent>
        </Card>
      )}

      {error && (
        <Alert variant="error" onClose={() => setError('')}>
          <div className="flex items-center gap-2">
            <AlertCircle className="h-4 w-4" />
            {error}
          </div>
        </Alert>
      )}

      {result && (
        <Alert variant="success" onClose={() => setResult(null)}>
          {result.mensaje}
        </Alert>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <Card className="lg:col-span-2">
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Brain className="h-5 w-5" />
              Entrenar Nuevo Modelo
            </CardTitle>
            <p className="text-sm text-muted-foreground mt-2">
              Ejecuta el entrenamiento real con los datos actuales de MariaDB
            </p>
          </CardHeader>

          <CardContent>
            <div className="space-y-6">
              <div>
                <label className="block text-sm mb-2">Seleccionar Algoritmo de ML</label>

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

                <p className="text-xs text-muted-foreground mt-2">
                  El backend ejecutará app/train_models.py y guardará los modelos en /models.
                </p>
              </div>

              <div className="p-4 bg-primary/5 border border-primary/20 rounded-lg">
                <div className="flex items-start gap-2">
                  <TrendingUp className="h-4 w-4 text-primary mt-0.5 flex-shrink-0" />
                  <div>
                    <p className="text-sm">Información</p>
                    <p className="text-xs text-muted-foreground mt-1">
                      Este proceso no es simulado. Llama al endpoint /api/admin/train del backend.
                    </p>
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
          </CardContent>
        </Card>

        <Card className="border-primary/20">
          <CardHeader>
            <CardTitle className="text-base">Modelo Activo Actual</CardTitle>
          </CardHeader>

          <CardContent className="space-y-4">
            <div className="flex items-center justify-between pb-3 border-b border-border">
              <span className="text-sm text-muted-foreground">Algoritmo</span>
              <Badge variant={result ? 'success' : 'secondary'}>
                {result?.modelo_nombre || getModelName(selectedModel)}
              </Badge>
            </div>

            <div className="flex items-center justify-between pb-3 border-b border-border">
              <span className="text-sm text-muted-foreground">Estado</span>
              <span className="text-sm">{result ? 'Entrenado' : 'Pendiente'}</span>
            </div>

            <div className="flex items-center justify-between pb-3 border-b border-border">
              <span className="text-sm text-muted-foreground">Precisión</span>
              <span className="text-sm">
                {result?.accuracy !== undefined ? `${result.accuracy}%` : 'Pendiente'}
              </span>
            </div>

            <div className="flex items-center justify-between pb-3 border-b border-border">
              <span className="text-sm text-muted-foreground">RMSE</span>
              <span className="text-sm">
                {result?.rmse !== undefined ? result.rmse : 'Pendiente'}
              </span>
            </div>

            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Último entrenamiento</span>
              <span className="text-sm">{result?.fecha || 'Pendiente'}</span>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Historial de Entrenamientos</CardTitle>
        </CardHeader>

        <CardContent>
          {trainings.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              Todavía no se ha ejecutado ningún entrenamiento real en esta sesión.
            </p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left py-3 px-4 text-sm">Fecha/Hora</th>
                    <th className="text-left py-3 px-4 text-sm">Modelo</th>
                    <th className="text-left py-3 px-4 text-sm">Accuracy</th>
                    <th className="text-left py-3 px-4 text-sm">RMSE</th>
                    <th className="text-left py-3 px-4 text-sm">Duración</th>
                    <th className="text-left py-3 px-4 text-sm">Estado</th>
                  </tr>
                </thead>

                <tbody>
                  {trainings.map((item) => (
                    <tr key={item.id} className="border-b border-border last:border-0 hover:bg-muted/50">
                      <td className="py-3 px-4 text-sm">{item.date}</td>
                      <td className="py-3 px-4 text-sm">{item.model}</td>
                      <td className="py-3 px-4 text-sm">{item.accuracy}</td>
                      <td className="py-3 px-4 text-sm">{item.rmse}</td>
                      <td className="py-3 px-4 text-sm">{item.duration}</td>
                      <td className="py-3 px-4">
                        <Badge variant="success">
                          <CheckCircle2 className="h-3 w-3 mr-1" />
                          {item.status}
                        </Badge>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
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
            Vas a ejecutar el entrenamiento real del modelo.
          </p>

          <div className="p-4 bg-muted rounded-lg space-y-2 text-sm">
            <div className="flex justify-between">
              <span className="text-muted-foreground">Algoritmo activo:</span>
              <span>{getModelName(selectedModel)}</span>
            </div>

            <div className="flex justify-between">
              <span className="text-muted-foreground">Script:</span>
              <span>app/train_models.py</span>
            </div>

            <div className="flex justify-between">
              <span className="text-muted-foreground">Datos:</span>
              <span>MariaDB</span>
            </div>
          </div>

          <p className="text-sm text-muted-foreground">
            Al terminar, los modelos se guardarán en la carpeta /models.
          </p>
        </div>
      </Modal>
    </div>
  );
}