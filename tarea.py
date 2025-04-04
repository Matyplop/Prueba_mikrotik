import pandas as pd
import streamlit as st
import librouteros
import re
from datetime import datetime, timedelta
import os
import csv

# Configuración y credenciales
USERNAME = st.secrets["credentials"]["username"]
PASSWORD = st.secrets["credentials"]["password"]
ROUTER_IP = st.secrets.get("router", {}).get("ip", "192.168.1.1")
ROUTER_USER = st.secrets.get("router", {}).get("username", "admin")
ROUTER_PASS = st.secrets.get("router", {}).get("password", "")

# Variables de estado
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
if "log_file" not in st.session_state:
    st.session_state.log_file = "pppoe_connection_log.csv"

# Función para conectar con el router MikroTik
def connect_to_mikrotik():
    try:
        api = librouteros.connect(
            username=ROUTER_USER,
            password=ROUTER_PASS,
            host=ROUTER_IP
        )
        return api
    except Exception as e:
        st.error(f"Error al conectar con el router: {str(e)}")
        return None

# Función para obtener usuarios PPPoE activos
def get_active_pppoe_users():
    api = connect_to_mikrotik()
    if not api:
        return {}
    
    try:
        # Obtener lista de usuarios PPPoE activos
        pppoe_active = api.path('/ppp/active')
        active_clients = {}

        for client in pppoe_active:
            if 'name' in client and 'address' in client:
                active_clients[client['name']] = {
                    'ip': client['address'],
                    'uptime': client.get('uptime', 'N/A'),
                    'caller_id': client.get('caller-id', 'N/A'),
                    'service': client.get('service', 'N/A')
                }

        return active_clients
    except Exception as e:
        st.error(f"Error al obtener usuarios activos: {str(e)}")
        return {}

# Función para obtener los logs del router MikroTik
def get_mikrotik_logs(time_minutes=60):
    api = connect_to_mikrotik()
    if not api:
        return []
    
    try:
        # Obtener logs del sistema
        log_path = api.path('/log')

        # Convertir a lista y limitar la cantidad de registros obtenidos
        logs = list(log_path)[:time_minutes * 5]  # Ajusta la cantidad de logs manualmente
        
        return logs
    except Exception as e:
        st.error(f"Error al obtener logs del router: {str(e)}")
        return []
    


# Función para filtrar logs de desconexión PPPoE
def filter_pppoe_disconnections(logs):
    disconnections = []
    pppoe_patterns = [
        r"pppoe (.*) disconnected",
        r"PPPoE connection closed for user (.*)",
        r"user (.*) disconnected",
        r"removed pppoe client (.*)",
        r"PPP user (.*) closed"
    ]
    
    now = datetime.now()
    
    for log in logs:
        if 'time' not in log or 'message' not in log:
            continue
        
        log_time = log.get('time', '')
        log_message = log.get('message', '')
        log_topics = log.get('topics', '')
        
        # Verificar si es un log relacionado con PPPoE
        is_pppoe_log = False
        if 'pppoe' in log_topics or 'ppp' in log_topics:
            is_pppoe_log = True
        else:
            for term in ['pppoe', 'ppp', 'disconnected', 'closed','terminating...']:
                if term in log_message.lower():
                    is_pppoe_log = True
                    break
        
        if not is_pppoe_log:
            continue
        
        # Buscar patrones de desconexión
        for pattern in pppoe_patterns:
            match = re.search(pattern, log_message, re.IGNORECASE)
            if match:
                username = match.group(1).strip()
                
                # Extraer la IP si está presente en el mensaje
                ip_match = re.search(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', log_message)
                ip = ip_match.group(1) if ip_match else 'N/A'
                
                disconnections.append({
                    'nombre': username,
                    'ip': ip,
                    'tiempo_desconexion': log_time,
                    'mensaje': log_message,
                    'topics': log_topics
                })
                break
    
    return disconnections

# Función para buscar desconexiones recientes
def find_recent_disconnections(time_minutes):
    logs = get_mikrotik_logs(time_minutes)
    
    if not logs:
        return []
    
    # Filtrar los logs para encontrar desconexiones PPPoE
    disconnections = filter_pppoe_disconnections(logs)
    
    # Guardar en el archivo de registro local
    save_disconnections_to_log(disconnections)
    
    return disconnections

# Función para guardar desconexiones en el registro local
def save_disconnections_to_log(disconnections):
    if not disconnections:
        return
    
    file_exists = os.path.isfile(st.session_state.log_file)
    
    with open(st.session_state.log_file, mode='a', newline='') as file:
        writer = csv.writer(file)
        
        # Escribir encabezados si es un archivo nuevo
        if not file_exists:
            writer.writerow(['Timestamp', 'Cliente', 'IP', 'Mensaje'])
        
        # Escribir las desconexiones
        for dc in disconnections:
            writer.writerow([
                dc.get('tiempo_desconexion', datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                dc.get('nombre', 'desconocido'),
                dc.get('ip', 'N/A'),
                dc.get('mensaje', 'N/A')
            ])

# Función para formatear datos para mostrar
def format_disconnections_for_display(disconnections):
    if not disconnections:
        return pd.DataFrame()
    
    # Crear dataframe
    df = pd.DataFrame(disconnections)
    
    # Renombrar columnas para mejor visualización
    columns_map = {
        'nombre': 'Cliente',
        'ip': 'IP',
        'tiempo_desconexion': 'Hora Desconexión',
        'mensaje': 'Mensaje del Log'
    }
    
    df_display = pd.DataFrame()
    for old_col, new_col in columns_map.items():
        if old_col in df.columns:
            df_display[new_col] = df[old_col]
    
    # Intentar ordenar por hora de desconexión si está disponible
    if 'Hora Desconexión' in df_display.columns:
        try:
            # Ordenar por la hora más reciente primero
            df_display = df_display.sort_values('Hora Desconexión', ascending=False)
        except:
            pass  # Si no se puede ordenar, mostrar como está
    
    return df_display

# Función principal
def main():
    try:
        st.set_page_config(
            page_title="Monitor de Desconexiones PPPoE",
            page_icon="📡",
            layout="wide",
        )
    except:
        pass

    # Mostrar login si no está autenticado
    if not st.session_state.authenticated:
        st.title("🔐 Inicio de Sesión")
        input_user = st.text_input("Usuario", value="", key="user")
        input_pass = st.text_input("Contraseña", value="", type="password", key="pass")

        if st.button("Ingresar"):
            if input_user == USERNAME and input_pass == PASSWORD:
                st.session_state.authenticated = True
                st.success("✅ Inicio de sesión exitoso.")
                st.rerun()
            else:
                st.error("❌ Usuario o contraseña incorrectos")
        st.stop()

    # Aplicación principal
    st.title("📡 Monitor de Desconexiones PPPoE")
    
    # Control para buscar desconexiones en un período específico
    time_options = {
        "15 minutos": 15,
        "30 minutos": 30,
        "1 hora": 60,
        "3 horas": 180,
        "6 horas": 360,
        "12 horas": 720,
        "1 día": 1440
    }
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.header("🔍 ¿Quién se desconectó?")
        
        selected_time = st.selectbox(
            "Buscar desconexiones de los últimos:",
            options=list(time_options.keys()),
            index=0  # Por defecto 15 minutos
        )
        
        # Botón para buscar desconexiones
        if st.button("🔍 Buscar desconexiones"):
            with st.spinner("Consultando logs del router..."):
                # Buscar desconexiones en los logs del router
                disconnections = find_recent_disconnections(time_options[selected_time])
                
                if disconnections:
                    st.success(f"✅ Se encontraron {len(disconnections)} desconexiones recientes")
                    
                    # Formatear para mostrar
                    df_display = format_disconnections_for_display(disconnections)
                    
                    # Mostrar la tabla
                    st.dataframe(df_display, use_container_width=True)
                    
                    # Opción para exportar datos
                    csv = df_display.to_csv(index=False)
                    st.download_button(
                        label="📥 Descargar datos como CSV",
                        data=csv,
                        file_name=f"desconexiones_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.success(f"✅ No se detectaron desconexiones en los últimos {selected_time}")
    
    with col2:
        st.header("⚙️ Opciones")
    
    # Inicializar session_state para reconexiones si no existe
        if "previous_disconnections" not in st.session_state:
            st.session_state.previous_disconnections = set()
        
        # Mostrar clientes activos actuales
        if st.button("👥 Ver clientes activos"):
            with st.spinner("Consultando router..."):
                active_clients = get_active_pppoe_users()
                if active_clients:
                    st.write(f"Clientes activos: **{len(active_clients)}**")
                    
                    clients_data = []
                    reconnected_clients = []

                    for name, data in active_clients.items():
                        clients_data.append({
                            "Cliente": name,
                            "IP": data.get('ip', 'N/A')
                        })

                        # Verificar si el usuario estaba en la lista de desconectados
                        if name in st.session_state.previous_disconnections:
                            reconnected_clients.append(name)

                    # Mostrar tabla con clientes activos
                    clients_df = pd.DataFrame(clients_data)
                    st.dataframe(clients_df, use_container_width=True)

                    # Mostrar mensaje si hay clientes que se reconectaron
                    if reconnected_clients:
                        st.success(f"✅ Se reconectaron los siguientes clientes: {', '.join(reconnected_clients)}")
                    
                    # Limpiar la lista de desconectados (solo mantiene los datos de la sesión actual)
                    st.session_state.previous_disconnections.clear()

                else:
                    st.warning("No se pudieron obtener los clientes activos")

        # Guardar los usuarios desconectados en session_state
        disconnections = find_recent_disconnections(15)  # Últimos 15 minutos
        if disconnections:
            disconnected_users = {dc['nombre'] for dc in disconnections}
            st.session_state.previous_disconnections.update(disconnected_users)

        # Opción para limpiar historial
        if st.button("🗑️ Limpiar historial"):
            if os.path.exists(st.session_state.log_file):
                os.remove(st.session_state.log_file)
                st.success("Historial de desconexiones limpiado")
                st.rerun()

if __name__ == "__main__":
    main()

