"""
test_api.py — Prueba integral NeoPlaylist Backend
-------------------------------------------------
Flujo:
1. Iniciar sesión (email/password)
2. Generar playlist desde prompt
3. Mostrar tracks obtenidos
4. Listar playlists guardadas

Genera logs detallados en test_log.json
"""

import argparse
import requests
import json
from datetime import datetime

API_BASE = "http://localhost:8000"
LOG_FILE = "test_log.json"


# =====================================================
# * Guardar log detallado
# =====================================================
def save_log(step: str, response):
    """Guarda en archivo el cuerpo de la respuesta para depuración."""
    data = {
        "timestamp": datetime.utcnow().isoformat(),
        "step": step,
        "status_code": response.status_code,
        "url": response.url,
        "response_text": response.text,
    }
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))
        f.write("\n\n")


# =====================================================
# * LOGIN
# =====================================================
def login(email: str, password: str):
    print("🔐 Iniciando sesión...")
    url = f"{API_BASE}/auth/login-password"
    payload = {"email": email, "password": password}

    resp = requests.post(url, json=payload)
    save_log("login", resp)

    if resp.status_code != 200:
        print(f"❌ Error en login: {resp.status_code} -> {resp.text}")
        return None

    try:
        data = resp.json()
    except Exception:
        print("❌ La respuesta no es JSON válida.")
        return None

    token = (
        data.get("token")  # ✅ tu backend devuelve este campo
        or data.get("access_token")
        or data.get("jwt")
        or data.get("accessToken")
    )

    if not token:
        print("⚠️ Login exitoso pero no se encontró token en la respuesta.")
        print("🧾 Respuesta completa:", json.dumps(data, indent=2, ensure_ascii=False))
    else:
        print(f"✅ Login exitoso -> {email}")

    return token


# =====================================================
# * GENERAR PLAYLIST
# =====================================================
def generate_playlist(token: str, prompt: str):
    print("\n🎧 Generando playlist con prompt:", prompt)
    headers = {"Authorization": f"Bearer {token}"}
    payload = {
        "mode": "hybrid",
        "prompt": prompt,
        "criteria": {},
        "name": "Test Playlist",
        "description": f"Generada automáticamente desde prompt: {prompt}",
    }

    # ✅ Endpoint correcto según tu Swagger
    resp = requests.post(f"{API_BASE}/playlist/query", json=payload, headers=headers)
    save_log("generate_playlist", resp)

    if resp.status_code == 200:
        print("✅ Playlist generada correctamente.\n")
        return resp.json()

    print(f"❌ Error generando playlist: {resp.status_code} -> {resp.text}")
    return None


# =====================================================
# * MOSTRAR TRACKS (ACTUALIZADO para estructura real)
# =====================================================
def show_playlist_tracks(playlist):
    print("🎵 --- PLAYLIST GENERADA ---")
    
    # 🔍 DEBUG: Mostrar estructura completa de la respuesta
    print("🔧 ESTRUCTURA DE RESPUESTA:")
    for key, value in playlist.items():
        if key != "playlist" and isinstance(value, (list, dict)):
            print(f"   {key}: {type(value)} - tamaño: {len(value) if isinstance(value, list) else 'dict'}")
        else:
            print(f"   {key}: {value}")

    # 🔍 Buscar pistas en diferentes estructuras
    items = playlist.get("playlist", [])
    
    if not items:
        print("❌ No se encontró campo 'playlist' con pistas")
        print("🔍 Buscando en otras estructuras...")
        
        # Buscar en posibles estructuras alternativas
        for key in ["tracks", "items", "data", "results"]:
            if key in playlist and isinstance(playlist[key], list):
                items = playlist[key]
                print(f"✅ Encontradas pistas en campo: {key}")
                break
    
    if not items:
        print("⚠️ No se recibieron pistas en ninguna estructura conocida")
        print("📋 Campos disponibles:", list(playlist.keys()))
        return

    print(f"🎵 Se encontraron {len(items)} pistas:\n")

    for i, track in enumerate(items, 1):
        if not isinstance(track, dict):
            print(f"Pista {i}: {track} (no es diccionario)")
            continue

        # Extraer campos con valores por defecto robustos
        titulo = track.get("Titulo") or track.get("title") or track.get("nombre") or "Desconocido"
        artista = track.get("Artista") or track.get("artist") or track.get("artista") or "Desconocido"
        album = track.get("Album") or track.get("album") or "N/A"
        anio = track.get("Año") or track.get("año") or track.get("year") or track.get("release_year") or "N/A"
        genero = track.get("Genero") or track.get("genero") or track.get("genre") or "N/A"
        
        # Manejar género como lista o string
        if isinstance(genero, list):
            genero = ", ".join([str(g) for g in genero])
        
        calidad = track.get("Calidad") or track.get("calidad") or track.get("bitrate") or "N/A"
        duracion = track.get("Duracion_mmss") or track.get("duracion") or track.get("duration") or "N/A"
        popularidad = track.get("PopularityDisplay") or track.get("popularity") or "N/A"

        print(f"{i:2d}. 🎶 {titulo}")
        print(f"    👤 {artista}")
        print(f"    💿 {album} | 📅 {anio} | 🎵 {genero}")
        print(f"    🎧 {calidad} | ⏱️ {duracion} | ⭐ {popularidad}")
        print("-" * 70)


# =====================================================
# * LISTAR PLAYLISTS GUARDADAS
# =====================================================
def list_user_playlists(token: str):
    print("\n📚 Consultando playlists guardadas...")
    headers = {"Authorization": f"Bearer {token}"}
    # ✅ Endpoint correcto: /playlist/
    resp = requests.get(f"{API_BASE}/playlist/", headers=headers)
    save_log("list_playlists", resp)

    if resp.status_code == 200:
        playlists = resp.json()
        print(f"✅ Se encontraron {len(playlists)} playlists guardadas:")
        for idx, p in enumerate(playlists, 1):
            print(f"  {idx}. {p.get('name')} - {p.get('description', '')}")
    else:
        print(f"❌ Error al obtener playlists: {resp.status_code} -> {resp.text}")


# =====================================================
# * MAIN
# =====================================================
def main():
    parser = argparse.ArgumentParser(description="Prueba integral NeoPlaylist")
    parser.add_argument("--email", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--prompt", required=True)
    args = parser.parse_args()

    print("🚀 Iniciando prueba integral de NeoPlaylist...\n")

    token = login(args.email, args.password)
    if not token:
        print("❌ No se pudo iniciar sesión. Abortando.")
        return

    playlist = generate_playlist(token, args.prompt)
    if playlist:
        show_playlist_tracks(playlist)

    list_user_playlists(token)

    print("\n✅ Prueba completada. Logs guardados en test_log.json")


if __name__ == "__main__":
    main()
