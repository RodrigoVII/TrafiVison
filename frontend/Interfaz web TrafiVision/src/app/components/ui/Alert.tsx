import { AlertCircle, CheckCircle2, Info, AlertTriangle, X } from 'lucide-react';

interface AlertProps {
  children: React.ReactNode;
  variant?: 'info' | 'success' | 'warning' | 'error';
  onClose?: () => void;
  className?: string;
}

export function Alert({ children, variant = 'info', onClose, className = '' }: AlertProps) {
  const variants = {
    info: {
      container: 'bg-primary/10 border-primary/20 text-primary',
      icon: <Info className="h-4 w-4" />,
    },
    success: {
      container: 'bg-success/10 border-success/20 text-success',
      icon: <CheckCircle2 className="h-4 w-4" />,
    },
    warning: {
      container: 'bg-warning/10 border-warning/20 text-warning',
      icon: <AlertTriangle className="h-4 w-4" />,
    },
    error: {
      container: 'bg-destructive/10 border-destructive/20 text-destructive',
      icon: <AlertCircle className="h-4 w-4" />,
    },
  };

  const style = variants[variant];

  return (
    <div className={`flex items-start gap-3 p-4 rounded-lg border ${style.container} ${className}`}>
      {style.icon}
      <div className="flex-1">{children}</div>
      {onClose && (
        <button onClick={onClose} className="hover:opacity-70">
          <X className="h-4 w-4" />
        </button>
      )}
    </div>
  );
}
