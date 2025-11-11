"""
run_server_with_logs.py — Ejecuta Uvicorn y guarda logs en archivo
"""

import subprocess
import sys
from datetime import datetime

def main():
    log_file = f"server_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    print(f"🚀 Iniciando servidor Uvicorn...")
    print(f"📝 Logs guardados en: {log_file}")
    
    # Comando para ejecutar Uvicorn
    cmd = [sys.executable, "-m", "uvicorn", "main:app", "--reload"]
    
    try:
        with open(log_file, "w", encoding="utf-8") as log_file_handle:
            # Escribir header
            log_file_handle.write(f"=== SERVER LOGS - {datetime.now().isoformat()} ===\n\n")
            log_file_handle.flush()
            
            # Ejecutar Uvicorn
            process = subprocess.Popen(
                cmd,
                stdout=log_file_handle,
                stderr=subprocess.STDOUT,
                text=True,
                encoding='utf-8'
            )
            
            print("✅ Servidor iniciado. Presiona Ctrl+C para detener.")
            process.wait()
            
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo servidor...")
        process.terminate()
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()