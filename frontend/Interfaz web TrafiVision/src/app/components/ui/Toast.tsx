import { useEffect } from 'react';
import { CheckCircle2, XCircle, AlertCircle, X } from 'lucide-react';

export interface ToastProps {
  message: string;
  type: 'success' | 'error' | 'info';
  onClose: () => void;
}

export function Toast({ message, type, onClose }: ToastProps) {
  useEffect(() => {
    const timer = setTimeout(onClose, 4000);
    return () => clearTimeout(timer);
  }, [onClose]);

  const styles = {
    success: {
      bg: 'bg-success text-white',
      icon: <CheckCircle2 className="h-5 w-5" />,
    },
    error: {
      bg: 'bg-destructive text-white',
      icon: <XCircle className="h-5 w-5" />,
    },
    info: {
      bg: 'bg-primary text-white',
      icon: <AlertCircle className="h-5 w-5" />,
    },
  };

  const style = styles[type];

  return (
    <div className={`fixed top-4 right-4 z-50 flex items-center gap-3 px-4 py-3 rounded-lg shadow-lg ${style.bg} min-w-[320px] animate-in slide-in-from-top-2`}>
      {style.icon}
      <p className="flex-1 text-sm">{message}</p>
      <button onClick={onClose} className="hover:opacity-70">
        <X className="h-4 w-4" />
      </button>
    </div>
  );
}
