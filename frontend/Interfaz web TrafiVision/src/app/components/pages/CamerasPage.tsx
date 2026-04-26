import { useEffect, useRef, useState } from 'react';
import { Camera, Search, MapPin, Eye, ImageOff } from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '../ui/Card';
import { Badge } from '../ui/Badge';
import { Button } from '../ui/Button';
import { Tooltip } from '../ui/Tooltip';

const API_URL = 'http://localhost:8000';

interface CameraData {
  id: number;
  codigo: string;
  nombre: string;
  zona: string;
  latitud: number | null;
  longitud: number | null;
  estado: string;
  ultima_captura: string;
}

export function CamerasPage() {
  const [searchTerm, setSearchTerm] = useState('');
  const [viewMode, setViewMode] = useState<'grid' | 'table'>('grid');
  const [selectedCamera, setSelectedCamera] = useState<CameraData | null>(null);
  const [cameras, setCameras] = useState<CameraData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const detailsRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    async function cargarCamaras() {
      try {
        setLoading(true);

        const response = await fetch(`${API_URL}/api/camaras`);

        if (!response.ok) {
          throw new Error('No se pudieron cargar las cámaras');
        }

        const data = await response.json();
        setCameras(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : 'Error cargando cámaras');
      } finally {
        setLoading(false);
      }
    }

    cargarCamaras();
  }, []);

  const abrirDetalles = (camera: CameraData) => {
    setSelectedCamera(camera);

    setTimeout(() => {
      detailsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 100);
  };

  const filteredCameras = cameras.filter((camera) =>
    camera.nombre.toLowerCase().includes(searchTerm.toLowerCase()) ||
    camera.codigo.toLowerCase().includes(searchTerm.toLowerCase()) ||
    camera.zona.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const getStatusColor = (status: string) => {
    return status === 'activa' ? 'success' : 'destructive';
  };

  const getStatusLabel = (status: string) => {
    return status === 'activa' ? 'Activa' : 'Sin datos';
  };

  if (loading) {
    return <p className="text-muted-foreground">Cargando cámaras desde MariaDB...</p>;
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
      {selectedCamera && (
        <div ref={detailsRef}>
          <Card className="border-primary">
            <CardHeader>
              <CardTitle>Detalles de Cámara</CardTitle>
            </CardHeader>

            <CardContent>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <div>
                  <div className="aspect-video bg-muted rounded-lg flex flex-col items-center justify-center mb-4">
                    <ImageOff className="h-12 w-12 text-muted-foreground mb-2" />
                    <p className="text-sm text-muted-foreground">Imagen no disponible todavía</p>
                  </div>

                  <p className="text-xs text-muted-foreground">
                    Las capturas se mostrarán aquí cuando el scraping guarde imágenes asociadas a esta cámara.
                  </p>
                </div>

                <div className="space-y-4">
                  <div className="p-4 bg-muted rounded-lg">
                    <p className="text-sm text-muted-foreground mb-1">Nombre</p>
                    <p className="text-sm">{selectedCamera.nombre}</p>
                  </div>

                  <div className="p-4 bg-muted rounded-lg">
                    <p className="text-sm text-muted-foreground mb-1">Código</p>
                    <p className="text-sm">{selectedCamera.codigo}</p>
                  </div>

                  <div className="p-4 bg-muted rounded-lg">
                    <p className="text-sm text-muted-foreground mb-1">Zona</p>
                    <p className="text-sm">{selectedCamera.zona}</p>
                  </div>

                  <div className="p-4 bg-muted rounded-lg">
                    <p className="text-sm text-muted-foreground mb-1">Estado actual</p>
                    <Badge variant={getStatusColor(selectedCamera.estado) as any}>
                      {getStatusLabel(selectedCamera.estado)}
                    </Badge>
                  </div>

                  <div className="p-4 bg-muted rounded-lg">
                    <p className="text-sm text-muted-foreground mb-1">Última captura</p>
                    <p className="text-sm">{selectedCamera.ultima_captura}</p>
                  </div>

                  <div className="p-4 bg-muted rounded-lg">
                    <p className="text-sm text-muted-foreground mb-1">Coordenadas</p>
                    <div className="flex items-center gap-2">
                      <MapPin className="h-4 w-4 text-muted-foreground" />
                      <p className="text-sm">
                        {selectedCamera.latitud && selectedCamera.longitud
                          ? `${selectedCamera.latitud}, ${selectedCamera.longitud}`
                          : 'Sin coordenadas registradas'}
                      </p>
                    </div>
                  </div>

                  <Button variant="secondary" className="w-full" onClick={() => setSelectedCamera(null)}>
                    Cerrar detalles
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        </div>
      )}

      <Card>
        <CardContent className="p-4">
          <div className="flex flex-col md:flex-row gap-4 items-center justify-between">
            <div className="relative flex-1 w-full">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-muted-foreground" />
              <input
                type="text"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                placeholder="Buscar por nombre, código o zona..."
                className="w-full pl-10 pr-4 py-2 bg-input-background border border-input rounded-lg focus:outline-none focus:ring-2 focus:ring-ring"
              />
            </div>

            <div className="flex gap-2">
              <Button variant={viewMode === 'grid' ? 'primary' : 'secondary'} size="sm" onClick={() => setViewMode('grid')}>
                Tarjetas
              </Button>
              <Button variant={viewMode === 'table' ? 'primary' : 'secondary'} size="sm" onClick={() => setViewMode('table')}>
                Tabla
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      {viewMode === 'grid' ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          {filteredCameras.map((camera) => (
            <Card key={camera.id} className="hover:shadow-md transition-shadow">
              <CardContent className="p-4">
                <div className="flex items-start justify-between mb-3">
                  <div className="flex items-center gap-2">
                    <Tooltip content={`Cámara ${camera.codigo} - ${camera.zona}`}>
                      <div className="w-10 h-10 rounded-lg bg-primary/10 flex items-center justify-center">
                        <Camera className="h-5 w-5 text-primary" />
                      </div>
                    </Tooltip>

                    <div>
                      <p className="text-sm">{camera.codigo}</p>
                      <p className="text-xs text-muted-foreground">{camera.zona}</p>
                    </div>
                  </div>

                  <Badge variant={getStatusColor(camera.estado) as any}>
                    {getStatusLabel(camera.estado)}
                  </Badge>
                </div>

                <h4 className="text-sm mb-3">{camera.nombre}</h4>

                <div className="flex items-center justify-between text-xs">
                  <span className="text-muted-foreground">Última captura</span>
                  <span>{camera.ultima_captura}</span>
                </div>

                <Button variant="ghost" size="sm" className="w-full mt-3" onClick={() => abrirDetalles(camera)}>
                  <Eye className="h-4 w-4" />
                  Ver Detalles
                </Button>
              </CardContent>
            </Card>
          ))}
        </div>
      ) : (
        <Card>
          <CardContent className="p-0">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-border">
                    <th className="text-left py-3 px-4 text-sm">Código</th>
                    <th className="text-left py-3 px-4 text-sm">Nombre</th>
                    <th className="text-left py-3 px-4 text-sm">Zona</th>
                    <th className="text-left py-3 px-4 text-sm">Estado</th>
                    <th className="text-left py-3 px-4 text-sm">Última Captura</th>
                    <th className="text-left py-3 px-4 text-sm">Acciones</th>
                  </tr>
                </thead>

                <tbody>
                  {filteredCameras.map((camera) => (
                    <tr key={camera.id} className="border-b border-border last:border-0 hover:bg-muted/50">
                      <td className="py-3 px-4 text-sm">{camera.codigo}</td>
                      <td className="py-3 px-4 text-sm">{camera.nombre}</td>
                      <td className="py-3 px-4 text-sm">{camera.zona}</td>
                      <td className="py-3 px-4">
                        <Badge variant={getStatusColor(camera.estado) as any}>
                          {getStatusLabel(camera.estado)}
                        </Badge>
                      </td>
                      <td className="py-3 px-4 text-sm">{camera.ultima_captura}</td>
                      <td className="py-3 px-4">
                        <Button variant="ghost" size="sm" onClick={() => abrirDetalles(camera)}>
                          <Eye className="h-4 w-4" />
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}