"""
test_suite_automated_v3.py — Test Suite con Análisis de Filtros y Logs
-----------------------------------------------------------------------
Mejoras:
1. Análisis detallado de filtros por década
2. Análisis de géneros
3. Integración con logs del servidor
4. Métricas de cumplimiento de filtros
"""

import requests
import json
import time
import logging
from datetime import datetime
import statistics
from typing import List, Dict, Any
import sys
import os

API_BASE = "http://localhost:8000"
TEST_REPORT_FILE = "test_automation_report_v3.json"
TEST_DETAIL_LOG_FILE = "test_detailed_results_v3.log"
FILTER_ANALYSIS_FILE = "filter_analysis_report.json"

# Configurar logging para el test
def setup_test_logging():
    """Configura logging detallado para el test suite."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(TEST_DETAIL_LOG_FILE, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger('test_suite')

class PlaylistTester:
    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.token = None
        self.test_results = []
        self.filter_analysis = []
        self.logger = setup_test_logging()
        
    def login(self):
        """Iniciar sesión una vez para todas las pruebas."""
        self.logger.info("🔐 Iniciando sesión...")
        url = f"{API_BASE}/auth/login-password"
        payload = {"email": self.email, "password": self.password}
        
        try:
            resp = requests.post(url, json=payload, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                self.token = data.get("token")
                if self.token:
                    self.logger.info(f"✅ Login exitoso -> {self.email}")
                    return True
                else:
                    self.logger.error("❌ No se pudo obtener token de la respuesta")
            else:
                self.logger.error(f"❌ Error HTTP en login: {resp.status_code}")
        except Exception as e:
            self.logger.error(f"❌ Excepción en login: {e}")
            
        return False
    
    def analyze_filters_compliance(self, tracks: List[Dict], prompt: str, test_name: str) -> Dict[str, Any]:
        """Analiza el cumplimiento de filtros temporales y de género."""
        if not tracks:
            return {
                "decade_compliance": 0,
                "year_range_compliance": 0,
                "genre_compliance": 0,
                "issues": ["No hay pistas para analizar"],
                "year_violations": [],
                "decade_violations": [],
                "genre_violations": []
            }
        
        # Extraer filtros del prompt
        prompt_lower = prompt.lower()
        
        # Detectar década solicitada
        decade_filters = []
        if "70s" in prompt_lower or "70" in prompt_lower:
            decade_filters.extend([1970, 1971, 1972, 1973, 1974, 1975, 1976, 1977, 1978, 1979])
        if "80s" in prompt_lower or "80" in prompt_lower:
            decade_filters.extend([1980, 1981, 1982, 1983, 1984, 1985, 1986, 1987, 1988, 1989])
        if "90s" in prompt_lower or "90" in prompt_lower:
            decade_filters.extend([1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999])
        if "2000s" in prompt_lower or "2000" in prompt_lower:
            decade_filters.extend([2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009])
        if "2010s" in prompt_lower or "2010" in prompt_lower:
            decade_filters.extend([2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019])
        if "2020s" in prompt_lower or "2020" in prompt_lower:
            decade_filters.extend([2020, 2021, 2022, 2023, 2024])
        
        # Detectar rango de años específico
        year_range = None
        import re
        year_matches = re.findall(r'(\d{4})-(\d{4})', prompt)
        if year_matches:
            start_year, end_year = map(int, year_matches[0])
            year_range = (start_year, end_year)
        
        # Detectar género solicitado
        genre_filters = []
        genre_keywords = ["rock", "pop", "jazz", "electrónica", "reggaeton", "clásica", "rap", "hip hop", "blues", "country"]
        for genre in genre_keywords:
            if genre in prompt_lower:
                genre_filters.append(genre)
        
        # Analizar cumplimiento
        decade_compliant = 0
        year_range_compliant = 0
        genre_compliant = 0
        total_tracks = len(tracks)
        
        year_violations = []
        decade_violations = []
        genre_violations = []
        
        for track in tracks:
            track_year = track.get("Año")
            track_genre = track.get("Genero", "")
            artist = track.get("Artista", "")
            title = track.get("Titulo", "")
            
            # Convertir género a string si es lista
            if isinstance(track_genre, list):
                track_genre = " ".join(str(g) for g in track_genre).lower()
            else:
                track_genre = str(track_genre).lower()
            
            # Verificar década
            if decade_filters and track_year:
                try:
                    year_int = int(float(track_year))
                    if year_int in decade_filters:
                        decade_compliant += 1
                    else:
                        decade_violations.append(f"{artist} - {title} ({track_year})")
                except (ValueError, TypeError):
                    decade_violations.append(f"{artist} - {title} (año inválido: {track_year})")
            
            # Verificar rango de años
            if year_range and track_year:
                try:
                    year_int = int(float(track_year))
                    if year_range[0] <= year_int <= year_range[1]:
                        year_range_compliant += 1
                    else:
                        year_violations.append(f"{artist} - {title} ({track_year})")
                except (ValueError, TypeError):
                    year_violations.append(f"{artist} - {title} (año inválido: {track_year})")
            
            # Verificar género
            if genre_filters:
                genre_match = any(genre in track_genre for genre in genre_filters)
                if genre_match:
                    genre_compliant += 1
                else:
                    genre_violations.append(f"{artist} - {title} (género: {track_genre})")
        
        # Calcular porcentajes
        decade_compliance = decade_compliant / total_tracks if decade_filters else 1.0
        year_range_compliance = year_range_compliant / total_tracks if year_range else 1.0
        genre_compliance = genre_compliant / total_tracks if genre_filters else 1.0
        
        issues = []
        if decade_filters and decade_compliance < 0.8:
            issues.append(f"Bajo cumplimiento década: {decade_compliance:.1%}")
        if year_range and year_range_compliance < 0.8:
            issues.append(f"Bajo cumplimiento rango años: {year_range_compliance:.1%}")
        if genre_filters and genre_compliance < 0.8:
            issues.append(f"Bajo cumplimiento género: {genre_compliance:.1%}")
        
        return {
            "decade_compliance": round(decade_compliance, 3),
            "year_range_compliance": round(year_range_compliance, 3),
            "genre_compliance": round(genre_compliance, 3),
            "issues": issues,
            "year_violations": year_violations[:5],  # Mostrar solo primeras 5 violaciones
            "decade_violations": decade_violations[:5],
            "genre_violations": genre_violations[:5],
            "detected_filters": {
                "decades": decade_filters,
                "year_range": year_range,
                "genres": genre_filters
            }
        }
    
    def generate_playlist(self, prompt: str, test_name: str) -> Dict[str, Any]:
        """Generar playlist con análisis detallado de filtros."""
        self.logger.info(f"\n🎧 TEST: {test_name}")
        self.logger.info(f"   Prompt: '{prompt}'")
        
        headers = {"Authorization": f"Bearer {self.token}"}
        payload = {
            "mode": "hybrid",
            "prompt": prompt,
            "criteria": {},
            "name": f"Test: {test_name}",
            "description": f"Generada automáticamente - {prompt}",
        }
        
        start_time = time.time()
        try:
            resp = requests.post(f"{API_BASE}/playlist/query", json=payload, headers=headers, timeout=120)
            response_time = time.time() - start_time
            
            if resp.status_code != 200:
                self.logger.error(f"   ❌ Error HTTP: {resp.status_code}")
                return self._create_error_result(test_name, prompt, f"HTTP {resp.status_code}", response_time)
            
            data = resp.json()
            
            # Buscar tracks en diferentes estructuras posibles
            tracks = data.get("playlist", [])
            if not tracks:
                tracks = data.get("tracks", [])
            if not tracks:
                tracks = data.get("items", [])
            if not tracks:
                tracks = data.get("results", [])
            
            if not tracks:
                self.logger.warning(f"   ⚠️ Playlist vacía")
                analysis = self.analyze_playlist_quality([], prompt)
                filter_analysis = self.analyze_filters_compliance([], prompt, test_name)
                return self._create_success_result(test_name, prompt, 0, response_time, analysis, filter_analysis, [])
            
            # Análisis de calidad y filtros
            analysis = self.analyze_playlist_quality(tracks, prompt)
            filter_analysis = self.analyze_filters_compliance(tracks, prompt, test_name)
            
            # Guardar análisis de filtros
            self.filter_analysis.append({
                "test_name": test_name,
                "prompt": prompt,
                **filter_analysis
            })
            
            # Mostrar detalles de las pistas con análisis de filtros
            self.logger.info(f"   🎵 Pistas encontradas ({len(tracks)}):")
            for i, track in enumerate(tracks[:8], 1):
                artist = track.get('Artista', 'Desconocido')
                title = track.get('Titulo', 'Sin título')
                year = track.get('Año', 'N/A')
                genre = track.get('Genero', 'N/A')
                if isinstance(genre, list):
                    genre = ", ".join(str(g) for g in genre)
                
                # Marcar violaciones de filtro
                violation_marker = ""
                if filter_analysis["decade_violations"] and f"{artist} - {title}" in [v.split(' (')[0] for v in filter_analysis["decade_violations"]]:
                    violation_marker = " ⚠️"
                elif filter_analysis["year_violations"] and f"{artist} - {title}" in [v.split(' (')[0] for v in filter_analysis["year_violations"]]:
                    violation_marker = " ⚠️"
                
                self.logger.info(f"      {i}. {artist} - {title} ({year}) [{genre}]{violation_marker}")
            
            if len(tracks) > 8:
                self.logger.info(f"      ... y {len(tracks) - 8} más")
            
            # Mostrar resumen de cumplimiento de filtros
            self.logger.info(f"   📊 CUMPLIMIENTO DE FILTROS:")
            if filter_analysis["detected_filters"]["decades"]:
                self.logger.info(f"      • Década: {filter_analysis['decade_compliance']:.1%}")
            if filter_analysis["detected_filters"]["year_range"]:
                self.logger.info(f"      • Rango años: {filter_analysis['year_range_compliance']:.1%}")
            if filter_analysis["detected_filters"]["genres"]:
                self.logger.info(f"      • Género: {filter_analysis['genre_compliance']:.1%}")
            
            if filter_analysis["issues"]:
                self.logger.warning(f"   ⚠️ PROBLEMAS FILTROS: {', '.join(filter_analysis['issues'])}")
            
            result = self._create_success_result(test_name, prompt, len(tracks), response_time, analysis, filter_analysis, tracks)
            
            self.logger.info(f"   ✅ Éxito: {len(tracks)} pistas, {response_time:.2f}s")
            self.logger.info(f"   📈 Calidad: {analysis.get('quality_score', 0)}/10")
            
            if analysis.get('issues'):
                self.logger.warning(f"   ⚠️ Problemas calidad: {', '.join(analysis['issues'])}")
            
            return result
            
        except requests.Timeout:
            self.logger.error(f"   ❌ Timeout en la solicitud (120s)")
            return self._create_error_result(test_name, prompt, "Timeout", time.time() - start_time)
        except Exception as e:
            self.logger.error(f"   ❌ Error procesando respuesta: {e}")
            return self._create_error_result(test_name, prompt, str(e), time.time() - start_time)
    
    def _create_success_result(self, test_name, prompt, track_count, response_time, analysis, filter_analysis, tracks):
        """Crea resultado exitoso con estructura robusta."""
        sample_tracks = []
        for track in tracks[:3]:
            artist = track.get('Artista', 'Desconocido')
            title = track.get('Titulo', 'Sin título')
            sample_tracks.append(f"{artist} - {title}")
        
        return {
            "test_name": test_name,
            "prompt": prompt,
            "success": True,
            "response_time": response_time,
            "tracks_count": track_count,
            "analysis": analysis,
            "filter_analysis": filter_analysis,
            "sample_tracks": sample_tracks,
            "all_tracks": [
                {
                    "artista": t.get('Artista', 'Desconocido'),
                    "titulo": t.get('Titulo', 'Sin título'),
                    "año": t.get('Año', 'N/A'),
                    "genero": t.get('Genero', 'N/A'),
                    "calidad": t.get('Calidad', 'N/A')
                } for t in tracks[:10]
            ]
        }
    
    def _create_error_result(self, test_name, prompt, error, response_time):
        """Crea resultado de error con estructura robusta."""
        return {
            "test_name": test_name,
            "prompt": prompt,
            "success": False,
            "error": error,
            "response_time": response_time,
            "tracks_count": 0,
            "analysis": {
                "quality_score": 0,
                "issues": [error],
                "artist_distribution": {},
                "unique_artists": 0,
                "relevance_ratio": 0,
                "duplicate_count": 0
            },
            "filter_analysis": {
                "decade_compliance": 0,
                "year_range_compliance": 0,
                "genre_compliance": 0,
                "issues": [error],
                "year_violations": [],
                "decade_violations": [],
                "genre_violations": []
            },
            "sample_tracks": [],
            "all_tracks": []
        }
    
    def analyze_playlist_quality(self, tracks: List[Dict], prompt: str) -> Dict[str, Any]:
        """Analiza la calidad de la playlist generada."""
        if not tracks:
            return {
                "quality_score": 0,
                "issues": ["Playlist vacía"],
                "artist_distribution": {},
                "unique_artists": 0,
                "relevance_ratio": 0,
                "duplicate_count": 0
            }
        
        issues = []
        score = 10
        
        try:
            # 1. Distribución de artistas
            artist_counts = {}
            for track in tracks:
                artist = track.get("Artista") or "Desconocido"
                if artist and isinstance(artist, str):
                    artist_counts[artist] = artist_counts.get(artist, 0) + 1
            
            if artist_counts:
                max_tracks_per_artist = max(artist_counts.values())
                if max_tracks_per_artist > len(tracks) * 0.4 and len(tracks) > 3:
                    dominant_artist = [k for k, v in artist_counts.items() if v == max_tracks_per_artist][0]
                    issues.append(f"Artista dominante: {dominant_artist} ({max_tracks_per_artist} pistas)")
                    score -= 2
            
            # 2. Verificar duplicados
            normalized_titles = set()
            duplicate_count = 0
            for track in tracks:
                title = track.get("Titulo")
                if title and isinstance(title, str):
                    normalized_title = self.normalize_title(title)
                    if normalized_title in normalized_titles:
                        duplicate_count += 1
                    normalized_titles.add(normalized_title)
            
            if duplicate_count > 0:
                issues.append(f"Duplicados: {duplicate_count}")
                score -= min(3, duplicate_count * 2)
            
            # 3. Penalizar playlists muy cortas
            if len(tracks) < 5 and "top 10" not in prompt.lower() and "5 mejores" not in prompt.lower():
                issues.append(f"Playlist corta: {len(tracks)} pistas")
                score -= (5 - len(tracks))
            
            score = max(0, min(10, score))
            
            return {
                "quality_score": round(score, 1),
                "issues": issues,
                "artist_distribution": dict(sorted(artist_counts.items(), key=lambda x: x[1], reverse=True)[:5]),
                "unique_artists": len(artist_counts),
                "relevance_ratio": 0,  # Ya no usamos este cálculo
                "duplicate_count": duplicate_count
            }
            
        except Exception as e:
            self.logger.error(f"Error en análisis de calidad: {e}")
            return {
                "quality_score": 5,
                "issues": [f"Error en análisis: {str(e)}"],
                "artist_distribution": {},
                "unique_artists": 0,
                "relevance_ratio": 0,
                "duplicate_count": 0
            }
    
    def normalize_title(self, title: str) -> str:
        """Normaliza título para detección de duplicados."""
        import re
        if not title or not isinstance(title, str):
            return ""
        normalized = re.sub(r"\s*[\[\(].*?[\]\)]", "", title.lower())
        normalized = re.sub(r"[^\w\s]", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip()
    
    def run_test_suite(self):
        """Ejecuta toda la suite de pruebas."""
        if not self.login():
            return
        
        test_scenarios = self.get_test_scenarios()
        
        self.logger.info(f"\n🚀 INICIANDO SUITE DE PRUEBAS - {len(test_scenarios)} escenarios")
        self.logger.info("=" * 80)
        
        for i, scenario in enumerate(test_scenarios, 1):
            self.logger.info(f"\n📋 Prueba {i}/{len(test_scenarios)}")
            result = self.generate_playlist(scenario["prompt"], scenario["name"])
            self.test_results.append(result)
            
            if i < len(test_scenarios):
                time.sleep(3)
        
        self.generate_report()
    
    def get_test_scenarios(self) -> List[Dict[str, str]]:
        """Define escenarios de prueba optimizados para testear filtros."""
        return [
            # 🔹 TEST CRÍTICOS - Filtros por década
            {"name": "Rock 70s-80s", "prompt": "rock clásico de los 70s y 80s - 15 canciones"},
            {"name": "Pop 2020-2024", "prompt": "pop actual 2020-2024 - 10 canciones"},
            {"name": "Años 90", "prompt": "música de los años 90 - 15 canciones"},
            {"name": "Años 2000", "prompt": "música de los años 2000 - 12 canciones"},
            
            # 🔹 TEST - Géneros específicos
            {"name": "Jazz Instrumental", "prompt": "jazz suave instrumental - 8 canciones"},
            {"name": "Rock Alternativo", "prompt": "rock alternativo - 10 canciones"},
            {"name": "Electrónica", "prompt": "música electrónica - 10 canciones"},
            
            # 🔹 TEST - Combinaciones
            {"name": "Pop 80s", "prompt": "pop de los años 80 - 12 canciones"},
            {"name": "Rock 2000s", "prompt": "rock de los años 2000 - 10 canciones"},
            
            # 🔹 TEST - Límites
            {"name": "Top 10 Rock", "prompt": "top 10 canciones de rock de todos los tiempos"},
            {"name": "5 Mejores Queen", "prompt": "5 mejores canciones de Queen"},
            
            # 🔹 TEST - Artistas
            {"name": "Michael Jackson", "prompt": "lo mejor de Michael Jackson - 15 canciones"},
            {"name": "The Beatles", "prompt": "canciones de The Beatles - 12 canciones"},
        ]
    
    def generate_report(self):
        """Genera reporte detallado con análisis de filtros."""
        try:
            successful_tests = [r for r in self.test_results if r.get("success")]
            failed_tests = [r for r in self.test_results if not r.get("success")]
            
            # Guardar análisis de filtros
            with open(FILTER_ANALYSIS_FILE, "w", encoding="utf-8") as f:
                json.dump(self.filter_analysis, f, ensure_ascii=False, indent=2)
            
            report = {
                "timestamp": datetime.utcnow().isoformat(),
                "summary": {
                    "total_tests": len(self.test_results),
                    "successful_tests": len(successful_tests),
                    "failed_tests": len(failed_tests),
                    "success_rate": len(successful_tests) / len(self.test_results) * 100 if self.test_results else 0,
                },
                "metrics": self._calculate_metrics(successful_tests),
                "filter_performance": self._calculate_filter_performance(),
                "detailed_results": self.test_results,
                "recommendations": self.generate_recommendations()
            }
            
            with open(TEST_REPORT_FILE, "w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            
            self.print_summary(report)
            
        except Exception as e:
            self.logger.error(f"Error generando reporte: {e}")
    
    def _calculate_filter_performance(self):
        """Calcula métricas de desempeño de filtros."""
        if not self.filter_analysis:
            return {}
        
        decade_compliances = [f["decade_compliance"] for f in self.filter_analysis if f["detected_filters"]["decades"]]
        year_range_compliances = [f["year_range_compliance"] for f in self.filter_analysis if f["detected_filters"]["year_range"]]
        genre_compliances = [f["genre_compliance"] for f in self.filter_analysis if f["detected_filters"]["genres"]]
        
        return {
            "avg_decade_compliance": round(statistics.mean(decade_compliances), 3) if decade_compliances else 0,
            "avg_year_range_compliance": round(statistics.mean(year_range_compliances), 3) if year_range_compliances else 0,
            "avg_genre_compliance": round(statistics.mean(genre_compliances), 3) if genre_compliances else 0,
            "total_filter_tests": len(self.filter_analysis)
        }
    
    def _calculate_metrics(self, successful_tests):
        """Calcula métricas con manejo de errores."""
        if not successful_tests:
            return {}
        
        try:
            quality_scores = [r.get("analysis", {}).get("quality_score", 0) for r in successful_tests]
            response_times = [r.get("response_time", 0) for r in successful_tests]
            track_counts = [r.get("tracks_count", 0) for r in successful_tests]
            
            return {
                "avg_quality_score": round(statistics.mean(quality_scores), 2),
                "avg_response_time": round(statistics.mean(response_times), 2),
                "avg_tracks_per_playlist": round(statistics.mean(track_counts), 1),
                "min_quality": min(quality_scores),
                "max_quality": max(quality_scores),
            }
        except:
            return {}
    
    def generate_recommendations(self):
        """Genera recomendaciones basadas en el análisis de filtros."""
        recommendations = []
        
        try:
            # Análisis de problemas de filtros
            poor_decade_tests = [f for f in self.filter_analysis if f["detected_filters"]["decades"] and f["decade_compliance"] < 0.7]
            poor_year_tests = [f for f in self.filter_analysis if f["detected_filters"]["year_range"] and f["year_range_compliance"] < 0.7]
            poor_genre_tests = [f for f in self.filter_analysis if f["detected_filters"]["genres"] and f["genre_compliance"] < 0.7]
            
            if poor_decade_tests:
                recommendations.append("🔴 CRÍTICO: Problemas graves con filtros por década")
                for test in poor_decade_tests[:3]:
                    recommendations.append(f"   - {test['test_name']}: {test['decade_compliance']:.1%} cumplimiento")
            
            if poor_year_tests:
                recommendations.append("🔴 CRÍTICO: Problemas graves con filtros por rango de años")
                for test in poor_year_tests[:2]:
                    recommendations.append(f"   - {test['test_name']}: {test['year_range_compliance']:.1%} cumplimiento")
            
            if poor_genre_tests:
                recommendations.append("🔸 MEJORAR: Problemas con filtros por género")
                for test in poor_genre_tests[:2]:
                    recommendations.append(f"   - {test['test_name']}: {test['genre_compliance']:.1%} cumplimiento")
            
            # Tests con errores
            error_tests = [r for r in self.test_results if not r.get("success")]
            if error_tests:
                recommendations.append("🔴 CORREGIR: Tests con errores de procesamiento")
                for test in error_tests[:3]:
                    recommendations.append(f"   - {test['test_name']}: {test.get('error', 'Error desconocido')}")
            
            if not recommendations:
                recommendations.append("✅ SISTEMA ESTABLE: Filtros funcionando correctamente")
                
        except Exception as e:
            recommendations.append(f"⚠️ Error generando recomendaciones: {e}")
        
        return recommendations
    
    def print_summary(self, report):
        """Imprime resumen ejecutivo en consola."""
        self.logger.info("\n" + "=" * 80)
        self.logger.info("📊 REPORTE EJECUTIVO - ANÁLISIS DE FILTROS")
        self.logger.info("=" * 80)
        
        summary = report["summary"]
        metrics = report["metrics"]
        filter_perf = report["filter_performance"]
        
        self.logger.info(f"📈 ESTADÍSTICAS GENERALES:")
        self.logger.info(f"   • Tests totales: {summary['total_tests']}")
        self.logger.info(f"   • Tests exitosos: {summary['successful_tests']}")
        self.logger.info(f"   • Tasa de éxito: {summary['success_rate']:.1f}%")
        self.logger.info(f"   • Calidad promedio: {metrics.get('avg_quality_score', 0)}/10")
        self.logger.info(f"   • Pistas por playlist: {metrics.get('avg_tracks_per_playlist', 0)}")
        
        self.logger.info(f"\n🎯 DESEMPEÑO DE FILTROS:")
        self.logger.info(f"   • Cumplimiento década: {filter_perf.get('avg_decade_compliance', 0):.1%}")
        self.logger.info(f"   • Cumplimiento rango años: {filter_perf.get('avg_year_range_compliance', 0):.1%}")
        self.logger.info(f"   • Cumplimiento género: {filter_perf.get('avg_genre_compliance', 0):.1%}")
        
        # Tests problemáticos por filtros
        poor_tests = [f for f in self.filter_analysis if 
                     (f["detected_filters"]["decades"] and f["decade_compliance"] < 0.7) or
                     (f["detected_filters"]["year_range"] and f["year_range_compliance"] < 0.7)]
        
        if poor_tests:
            self.logger.info(f"\n⚠️  TESTS CON PROBLEMAS DE FILTROS:")
            for test in poor_tests[:3]:
                issues = []
                if test["detected_filters"]["decades"] and test["decade_compliance"] < 0.7:
                    issues.append(f"década: {test['decade_compliance']:.1%}")
                if test["detected_filters"]["year_range"] and test["year_range_compliance"] < 0.7:
                    issues.append(f"años: {test['year_range_compliance']:.1%}")
                self.logger.info(f"   • {test['test_name']}: {', '.join(issues)}")
        
        self.logger.info(f"\n💡 RECOMENDACIONES:")
        for rec in report["recommendations"]:
            self.logger.info(f"   {rec}")
        
        self.logger.info(f"\n📄 ARCHIVOS GENERADOS:")
        self.logger.info(f"   • Reporte completo: {TEST_REPORT_FILE}")
        self.logger.info(f"   • Análisis filtros: {FILTER_ANALYSIS_FILE}")
        self.logger.info(f"   • Logs detallados: {TEST_DETAIL_LOG_FILE}")
        self.logger.info("=" * 80)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Test Suite con Análisis de Filtros v3")
    parser.add_argument("--email", required=True, help="Email de usuario")
    parser.add_argument("--password", required=True, help="Contraseña")
    
    args = parser.parse_args()
    
    print("🚀 Test Suite NeoPlaylist v3 - Análisis de Filtros")
    print("🎯 Detecta problemas de década, año y género")
    print("=" * 60)
    
    tester = PlaylistTester(args.email, args.password)
    tester.run_test_suite()

if __name__ == "__main__":
    main()